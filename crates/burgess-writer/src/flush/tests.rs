//! Flushes against an in-memory store that records every write and can be
//! told to fail one, to check the write order and what each failure leaves.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::{DayManifest, FileStats, StatesBounds, WriterId};
use burgess_core::wire::{PayloadType, UnixMillis};
use bytes::Bytes;
use futures::stream::BoxStream;
use jiff::Timestamp;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

use super::*;
use crate::batch::{RefRow, SortedBatch, StateEntry, StateRow};
use crate::parquet_files::{FileWriter, ParquetConfig};
use crate::source::{PartitionId, Position};

/// An in-memory store that remembers the order of writes and can fail the
/// write to one object.
#[derive(Debug, Default)]
struct RecordingStore {
    inner: InMemory,
    writes: Mutex<Vec<String>>,
    reads: Mutex<Vec<String>>,
    /// Writes to a path ending with this fail.
    fail_writes_to: Mutex<Option<String>>,
}

impl RecordingStore {
    fn writes(&self) -> Vec<String> {
        self.writes.lock().unwrap().clone()
    }

    fn reads(&self) -> Vec<String> {
        self.reads.lock().unwrap().clone()
    }

    fn fail_writes_to(&self, suffix: Option<&str>) {
        *self.fail_writes_to.lock().unwrap() = suffix.map(String::from);
    }
}

impl fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingStore({})", self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let failing = self.fail_writes_to.lock().unwrap().clone();
        if failing.is_some_and(|suffix| location.as_ref().ends_with(&suffix)) {
            return Err(object_store::Error::Generic {
                store: "RecordingStore",
                source: "injected failure".into(),
            });
        }
        self.writes.lock().unwrap().push(location.to_string());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.reads.lock().unwrap().push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// A clock the test sets by hand.
#[derive(Clone)]
struct TestClock(Arc<Mutex<Timestamp>>);

impl TestClock {
    fn at(time: &str) -> Self {
        TestClock(Arc::new(Mutex::new(time.parse().unwrap())))
    }

    fn set(&self, time: &str) {
        *self.0.lock().unwrap() = time.parse().unwrap();
    }
}

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        *self.0.lock().unwrap()
    }
}

const FIRST_FLUSH: &str = "2026-09-27T10:00:00Z";
const FIRST_NNNN: u64 = 1_790_503_200_000;
const SECOND_FLUSH: &str = "2026-09-27T10:10:00Z";

fn writer_id() -> WriterId {
    "writer-1".parse().unwrap()
}

async fn flusher(store: &Arc<RecordingStore>, clock: &TestClock) -> Flusher<TestClock> {
    Flusher::open(
        Arc::clone(store) as Arc<dyn ObjectStore>,
        FileWriter::new(ParquetConfig::default()).unwrap(),
        clock.clone(),
        writer_id(),
    )
    .await
    .unwrap()
}

fn positions(pairs: &[(u32, u64)]) -> BTreeMap<PartitionId, Position> {
    pairs
        .iter()
        .map(|(partition, position)| (PartitionId::from(*partition), Position::from(*position)))
        .collect()
}

fn batch(positions: BTreeMap<PartitionId, Position>) -> Arc<SortedBatch> {
    let state = |tree: u8, ms: u64| StateRow {
        tree: TreeId::from([tree; 12]),
        subflow: Subflow::ROOT,
        time: UnixMillis::from(ms),
        service: "checkout".into(),
        category: "payment".into(),
        entry: StateEntry::State {
            meta: BTreeMap::new(),
            payload_type: PayloadType::Json,
            payload: b"{}".to_vec(),
        },
    };
    Arc::new(SortedBatch {
        states: vec![state(1, 300), state(4, 100)],
        refs: vec![RefRow {
            ref_type: "order_id".into(),
            domain_id: "ORD-1".into(),
            service: "orders".into(),
            category: None,
            tree: TreeId::from([1; 12]),
            subflow: Subflow::ROOT,
            time: UnixMillis::from(300),
        }],
        positions,
        skipped: 0,
    })
}

async fn read(store: &RecordingStore, path: &str) -> Option<Bytes> {
    match store.get(&Path::from(path)).await {
        Ok(stored) => Some(stored.bytes().await.unwrap()),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(error) => panic!("reading {path}: {error}"),
    }
}

async fn manifest(store: &RecordingStore, day: &str) -> Option<DayManifest> {
    let bytes = read(store, &format!("{day}/_manifest")).await?;
    Some(DayManifest::decode(&bytes).unwrap())
}

async fn stored_checkpoint(store: &RecordingStore) -> Option<Checkpoint> {
    let bytes = read(store, "_checkpoint/writer-1.json").await?;
    Some(Checkpoint::from_json(&bytes).unwrap())
}

#[tokio::test]
async fn writes_files_manifest_and_checkpoint_in_order() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch(positions(&[(0, 10)])))
        .await
        .unwrap();
    assert_eq!(
        store.writes(),
        [
            format!("20260927/{FIRST_NNNN}-states.parquet"),
            format!("20260927/{FIRST_NNNN}-refs.parquet"),
            "20260927/_manifest".to_string(),
            "_checkpoint/writer-1.json".to_string(),
        ]
    );
}

#[tokio::test]
async fn manifest_entry_describes_the_flush() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let entry = flusher(&store, &clock)
        .await
        .flush(batch(positions(&[(0, 10)])))
        .await
        .unwrap();

    let manifest = manifest(&store, "20260927").await.unwrap();
    assert_eq!(manifest.writer_id, writer_id());
    assert!(!manifest.sealed);
    assert_eq!(manifest.entries, std::slice::from_ref(&entry));
    assert_eq!(entry.nnnn, UnixMillis::from(FIRST_NNNN));
    assert_eq!(entry.seq, 1);
    assert_eq!(
        entry.states_bounds,
        Some(StatesBounds {
            min_tree: TreeId::from([1; 12]),
            max_tree: TreeId::from([4; 12]),
            min_ts: UnixMillis::from(100),
            max_ts: UnixMillis::from(300),
        })
    );
    let states = read(&store, &format!("20260927/{FIRST_NNNN}-states.parquet"))
        .await
        .unwrap();
    assert_eq!(
        entry.states,
        FileStats {
            rows: 2,
            bytes: states.len() as u64
        }
    );
    assert_eq!(entry.refs.rows, 1);
}

#[tokio::test]
async fn checkpoint_records_seq_positions_and_flush_time() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher
        .flush(batch(positions(&[(0, 10), (1, 4)])))
        .await
        .unwrap();

    let expected = Checkpoint {
        seq: 1,
        positions: positions(&[(0, 10), (1, 4)]),
        last_flush_nnnn: Some(UnixMillis::from(FIRST_NNNN)),
    };
    assert_eq!(stored_checkpoint(&store).await, Some(expected.clone()));
    assert_eq!(flusher.checkpoint(), &expected);
}

#[tokio::test]
async fn next_flush_appends_and_merges_positions() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher
        .flush(batch(positions(&[(0, 10), (1, 4)])))
        .await
        .unwrap();
    clock.set(SECOND_FLUSH);
    flusher.flush(batch(positions(&[(1, 9)]))).await.unwrap();

    let seqs: Vec<u64> = manifest(&store, "20260927")
        .await
        .unwrap()
        .entries
        .iter()
        .map(|entry| entry.seq)
        .collect();
    assert_eq!(seqs, [1, 2]);
    let checkpoint = stored_checkpoint(&store).await.unwrap();
    assert_eq!(checkpoint.seq, 2);
    assert_eq!(checkpoint.positions, positions(&[(0, 10), (1, 9)]));
}

#[tokio::test]
async fn empty_batch_writes_both_files_and_an_entry_without_bounds() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let entry = flusher(&store, &clock)
        .await
        .flush(Arc::default())
        .await
        .unwrap();
    assert_eq!(store.writes().len(), 4);
    assert_eq!(entry.states_bounds, None);
    assert_eq!((entry.states.rows, entry.refs.rows), (0, 0));
    assert!(entry.states.bytes > 0 && entry.refs.bytes > 0);
}

#[tokio::test]
async fn flush_after_midnight_opens_the_next_day() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-27T23:59:59Z");
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    clock.set("2026-09-28T00:09:59Z");
    flusher.flush(batch(positions(&[(0, 2)]))).await.unwrap();

    assert_eq!(manifest(&store, "20260927").await.unwrap().entries.len(), 1);
    let next_day = manifest(&store, "20260928").await.unwrap();
    assert_eq!(next_day.entries.len(), 1);
    assert_eq!(next_day.entries[0].seq, 2);
}

#[tokio::test]
async fn failure_before_the_manifest_leaves_invisible_files_and_can_be_retried() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    store.fail_writes_to(Some("-refs.parquet"));
    assert!(matches!(
        flusher.flush(batch(positions(&[(0, 10)]))).await,
        Err(FlushError::Store(_))
    ));

    assert!(
        read(&store, &format!("20260927/{FIRST_NNNN}-states.parquet"))
            .await
            .is_some(),
        "the states file stays as an orphan"
    );
    assert_eq!(manifest(&store, "20260927").await, None);
    assert_eq!(stored_checkpoint(&store).await, None);

    store.fail_writes_to(None);
    clock.set(SECOND_FLUSH);
    let entry = flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();
    assert_eq!(entry.seq, 1);
    assert_eq!(manifest(&store, "20260927").await.unwrap().entries, [entry]);
}

#[tokio::test]
async fn failure_before_the_checkpoint_keeps_the_old_offsets() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();
    let before = stored_checkpoint(&store).await.unwrap();

    store.fail_writes_to(Some(".json"));
    clock.set(SECOND_FLUSH);
    assert!(matches!(
        flusher.flush(batch(positions(&[(0, 20)]))).await,
        Err(FlushError::Store(_))
    ));
    assert_eq!(stored_checkpoint(&store).await, Some(before.clone()));
    assert_eq!(flusher.checkpoint(), &before);
    assert_eq!(manifest(&store, "20260927").await.unwrap().entries.len(), 2);
}

#[tokio::test]
async fn failure_at_the_manifest_or_later_needs_a_restart() {
    for failing in ["_manifest", ".json"] {
        let store = Arc::new(RecordingStore::default());
        let clock = TestClock::at(FIRST_FLUSH);
        let mut flusher = flusher(&store, &clock).await;
        store.fail_writes_to(Some(failing));
        assert!(flusher.flush(batch(positions(&[(0, 10)]))).await.is_err());

        store.fail_writes_to(None);
        assert!(
            matches!(
                flusher.flush(batch(positions(&[(0, 10)]))).await,
                Err(FlushError::NeedsRestart)
            ),
            "after failing at {failing}"
        );
    }
}

#[tokio::test]
async fn reopening_continues_from_the_stored_checkpoint() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch(positions(&[(0, 10)])))
        .await
        .unwrap();

    clock.set(SECOND_FLUSH);
    let mut reopened = flusher(&store, &clock).await;
    assert_eq!(reopened.checkpoint().seq, 1);
    let entry = reopened.flush(batch(positions(&[(0, 15)]))).await.unwrap();
    assert_eq!(entry.seq, 2);
}

#[tokio::test]
async fn unreadable_checkpoint_is_an_error_not_a_fresh_start() {
    let store = Arc::new(RecordingStore::default());
    store
        .put(
            &Path::from("_checkpoint/writer-1.json"),
            b"not json".to_vec().into(),
        )
        .await
        .unwrap();
    let opened = Flusher::open(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        FileWriter::new(ParquetConfig::default()).unwrap(),
        TestClock::at(FIRST_FLUSH),
        writer_id(),
    )
    .await;
    assert!(matches!(opened, Err(FlushError::Checkpoint(_))));
}

#[tokio::test]
async fn refuses_to_add_to_a_sealed_day() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();

    let mut sealed = manifest(&store, "20260927").await.unwrap();
    sealed.sealed = true;
    store
        .put(
            &Path::from("20260927/_manifest"),
            sealed.encode().unwrap().into(),
        )
        .await
        .unwrap();

    // A fresh flusher reads the sealed manifest from the store.
    let mut flusher = self::flusher(&store, &clock).await;
    clock.set(SECOND_FLUSH);
    let writes = store.writes().len();
    assert!(matches!(
        flusher.flush(batch(positions(&[(0, 20)]))).await,
        Err(FlushError::SealedDay { .. })
    ));
    assert_eq!(store.writes().len(), writes, "nothing written");
    assert_eq!(manifest(&store, "20260927").await, Some(sealed));
}

#[tokio::test]
async fn clock_going_back_never_reuses_a_flush_time() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(SECOND_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    let first = flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    clock.set(FIRST_FLUSH);
    let second = flusher.flush(batch(positions(&[(0, 2)]))).await.unwrap();
    let third = flusher.flush(batch(positions(&[(0, 3)]))).await.unwrap();

    assert_eq!(u64::from(second.nnnn), u64::from(first.nnnn) + 1);
    assert_eq!(u64::from(third.nnnn), u64::from(first.nnnn) + 2);
    let data_files = store
        .writes()
        .into_iter()
        .filter(|path| path.ends_with(".parquet"))
        .collect::<Vec<_>>();
    let unique: std::collections::BTreeSet<_> = data_files.iter().collect();
    assert_eq!(unique.len(), data_files.len(), "no data file written twice");
}

#[tokio::test]
async fn clock_going_back_across_midnight_stays_on_the_current_day() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-28T00:00:05Z");
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    clock.set("2026-09-27T23:59:58Z");
    flusher.flush(batch(positions(&[(0, 2)]))).await.unwrap();
    assert_eq!(manifest(&store, "20260928").await.unwrap().entries.len(), 2);
    assert_eq!(manifest(&store, "20260927").await, None);
}

#[tokio::test]
async fn refuses_a_clock_before_1970() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("1969-12-31T23:59:59Z");
    let mut flusher = flusher(&store, &clock).await;
    assert!(matches!(
        flusher.flush(batch(positions(&[(0, 1)]))).await,
        Err(FlushError::ClockBeforeEpoch { .. })
    ));
    assert!(store.writes().is_empty());
}

#[tokio::test]
async fn refuses_a_manifest_of_another_writer() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let other: WriterId = "writer-2".parse().unwrap();
    let foreign = DayManifest::new("20260927".parse().unwrap(), other.clone());
    store
        .put(
            &Path::from("20260927/_manifest"),
            foreign.encode().unwrap().into(),
        )
        .await
        .unwrap();

    let mut flusher = flusher(&store, &clock).await;
    let writes = store.writes().len();
    let refused = flusher.flush(batch(positions(&[(0, 1)]))).await;
    assert!(
        matches!(&refused, Err(FlushError::ForeignManifest { owner, .. }) if *owner == other),
        "{refused:?}"
    );
    assert_eq!(store.writes().len(), writes, "nothing written");
}

#[tokio::test]
async fn reads_the_day_manifest_once_per_day() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    for (minute, position) in [(0, 1), (10, 2), (20, 3)] {
        clock.set(&format!("2026-09-27T10:{minute:02}:00Z"));
        flusher
            .flush(batch(positions(&[(0, position)])))
            .await
            .unwrap();
    }
    let manifest_reads = store
        .reads()
        .iter()
        .filter(|path| path.ends_with("_manifest"))
        .count();
    assert_eq!(manifest_reads, 1);
}
