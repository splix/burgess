//! Days of files and manifests, stored as the writer leaves them.

use std::sync::Arc;

use burgess_core::flow::TreeId;
use burgess_core::storage::{
    Day, DayManifest, FileStats, FlushEntry, ObjectName, RefRow, StateRow, StatesBounds,
};
use burgess_core::wire::UnixMillis;
use burgess_writer::batch::SortedBatch;
use burgess_writer::parquet_files::{FileWriter, ParquetConfig};
use jiff::Timestamp;
use jiff::civil::Date;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use super::RecordingStore;
use crate::day_scan::ScanConfig;
use crate::manifests::{ManifestCache, ManifestCacheConfig};
use crate::pages::PageLimits;
use crate::parquet_file::FileReadConfig;
use crate::queries::Queries;
use crate::refs_file::RefsFileReader;
use crate::refs_lookup::RefsLookup;
use crate::states_file::StatesFileReader;
use crate::states_lookup::StatesLookup;
use crate::tree_lookup::{TreeLookup, TreeLookupConfig};

pub(crate) fn noon(day: Day) -> Timestamp {
    Date::from(day)
        .at(12, 0, 0, 0)
        .to_zoned(jiff::tz::TimeZone::UTC)
        .unwrap()
        .timestamp()
}

/// A tree minted at noon of `day`; `random` orders trees of the same day.
pub(crate) fn minted_on(day: Day, random: u8) -> TreeId {
    let millis = noon(day).as_millisecond().to_be_bytes();
    let mut id = [random; 12];
    id[..6].copy_from_slice(&millis[2..]);
    TreeId::from(id)
}

/// `offset` milliseconds past noon of `day`, in unix milliseconds.
pub(crate) fn at(day: Day, offset: u64) -> u64 {
    noon(day).as_millisecond() as u64 + offset
}

/// The rows of one flush, in the order its states file stores them.
pub(crate) fn flush(parts: Vec<Vec<StateRow>>) -> Vec<StateRow> {
    let mut rows: Vec<StateRow> = parts.into_iter().flatten().collect();
    rows.sort_by_key(StateRow::sort_key);
    rows
}

/// A store holding days of flushes, which remembers what was read from it.
pub(crate) struct Days {
    pub(crate) store: Arc<RecordingStore>,
}

impl Days {
    pub(crate) fn new() -> Self {
        Days {
            store: Arc::new(RecordingStore::default()),
        }
    }

    /// Stores `flushes` as the files and manifest of `day`, oldest first.
    pub(crate) async fn put(&self, day: Day, flushes: &[&[StateRow]]) {
        put_day(&self.store.inner, day, flushes).await;
    }

    /// Stores flushes of only references as the files and manifest of
    /// `day`, oldest first. The references of a flush are sorted as the
    /// writer sorts them.
    pub(crate) async fn put_refs(&self, day: Day, flushes: &[&[RefRow]]) {
        let flushes: Vec<Flush> = flushes
            .iter()
            .map(|refs| {
                let mut refs = refs.to_vec();
                refs.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
                Flush {
                    states: Vec::new(),
                    refs,
                }
            })
            .collect();
        put_flushes(&self.store.inner, day, &flushes).await;
    }

    pub(crate) async fn put_object(&self, name: &ObjectName, bytes: Vec<u8>) {
        put_object(&self.store.inner, name, bytes).await;
    }

    /// A lookup over the stored days, with default settings for the
    /// manifests and the files.
    pub(crate) fn lookup(&self, config: TreeLookupConfig) -> TreeLookup {
        let manifests = ManifestCache::new(self.store.clone(), ManifestCacheConfig::default());
        let files = StatesFileReader::new(self.store.clone(), FileReadConfig::default());
        TreeLookup::new(Arc::new(manifests), files, config)
    }

    /// A refs lookup over the stored days, with default settings for the
    /// manifests and the files.
    pub(crate) fn refs_lookup(&self, config: ScanConfig) -> RefsLookup {
        let manifests = ManifestCache::new(self.store.clone(), ManifestCacheConfig::default());
        let files = RefsFileReader::new(self.store.clone(), FileReadConfig::default());
        RefsLookup::new(Arc::new(manifests), files, config)
    }

    /// A states lookup over the stored days, with default settings for the
    /// manifests and the files.
    pub(crate) fn states_lookup(&self, config: ScanConfig) -> StatesLookup {
        let manifests = ManifestCache::new(self.store.clone(), ManifestCacheConfig::default());
        let files = StatesFileReader::new(self.store.clone(), FileReadConfig::default());
        StatesLookup::new(Arc::new(manifests), files, config)
    }

    /// The queries over the stored days, with default settings.
    pub(crate) fn queries(&self) -> Queries {
        self.queries_with(ManifestCacheConfig::default())
    }

    /// The queries over the stored days, whose lookups share one cache of
    /// manifests with `config`. A shorter retention is the retention of a
    /// later day.
    pub(crate) fn queries_with(&self, config: ManifestCacheConfig) -> Queries {
        let manifests = Arc::new(ManifestCache::new(self.store.clone(), config));
        let states = || StatesFileReader::new(self.store.clone(), FileReadConfig::default());
        let refs = RefsFileReader::new(self.store.clone(), FileReadConfig::default());
        Queries {
            lookup: TreeLookup::new(manifests.clone(), states(), TreeLookupConfig::default()),
            refs: RefsLookup::new(manifests.clone(), refs, ScanConfig::default()),
            states: StatesLookup::new(manifests.clone(), states(), ScanConfig::default()),
            manifests,
            limits: PageLimits::default(),
        }
    }

    /// The days whose manifest was requested, in request order.
    pub(crate) fn manifest_days(&self) -> Vec<Day> {
        self.store
            .paths()
            .iter()
            .filter(|path| path.ends_with("_manifest"))
            .map(|path| path[..8].parse().unwrap())
            .collect()
    }

    /// The objects requested under `day`, one entry per request.
    pub(crate) fn requests_on(&self, day: Day) -> Vec<String> {
        let prefix = format!("{day}/");
        self.store
            .paths()
            .into_iter()
            .filter(|path| path.starts_with(&prefix))
            .collect()
    }
}

/// Stores `flushes` in `store` as the files and manifest of `day`, oldest
/// first.
pub(crate) async fn put_day(store: &dyn ObjectStore, day: Day, flushes: &[&[StateRow]]) {
    let flushes: Vec<Flush> = flushes
        .iter()
        .map(|states| Flush {
            states: states.to_vec(),
            refs: Vec::new(),
        })
        .collect();
    put_flushes(store, day, &flushes).await;
}

/// The sorted rows of one flush.
struct Flush {
    states: Vec<StateRow>,
    refs: Vec<RefRow>,
}

async fn put_flushes(store: &dyn ObjectStore, day: Day, flushes: &[Flush]) {
    let writer = FileWriter::new(ParquetConfig::default()).unwrap();
    let mut manifest = DayManifest::new(day, "writer-1".parse().unwrap());
    for (seq, flush) in flushes.iter().enumerate() {
        let nnnn = UnixMillis::from(at(day, seq as u64));
        let files = writer
            .write_batch(&SortedBatch {
                states: flush.states.clone(),
                refs: flush.refs.clone(),
                ..SortedBatch::default()
            })
            .unwrap();
        manifest.entries.push(FlushEntry {
            nnnn,
            seq: seq as u64,
            states_bounds: bounds(&flush.states),
            states: FileStats {
                rows: files.states.rows as u64,
                bytes: files.states.bytes.len() as u64,
            },
            refs: FileStats {
                rows: files.refs.rows as u64,
                bytes: files.refs.bytes.len() as u64,
            },
        });
        put_object(
            store,
            &ObjectName::StatesFile { day, nnnn },
            files.states.bytes,
        )
        .await;
        put_object(store, &ObjectName::RefsFile { day, nnnn }, files.refs.bytes).await;
    }
    put_object(
        store,
        &ObjectName::Manifest { day },
        manifest.encode().unwrap(),
    )
    .await;
}

async fn put_object(store: &dyn ObjectStore, name: &ObjectName, bytes: Vec<u8>) {
    store
        .put(&Path::from(name.to_string()), bytes.into())
        .await
        .unwrap();
}

fn bounds(rows: &[StateRow]) -> Option<StatesBounds> {
    Some(StatesBounds {
        min_tree: rows.iter().map(|row| row.tree).min()?,
        max_tree: rows.iter().map(|row| row.tree).max()?,
        min_ts: rows.iter().map(|row| row.time).min()?,
        max_ts: rows.iter().map(|row| row.time).max()?,
    })
}
