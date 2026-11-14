//! Flushes against an in-memory store that records every write and can be
//! told to fail one, to check the write order and what each failure leaves.

use std::collections::BTreeMap;
use std::sync::Arc;

use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::{DayManifest, FileStats, StatesBounds, WriterId};
use burgess_core::wire::{PayloadType, UnixMillis};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use super::*;
use crate::batch::{RefRow, SortedBatch, StateEntry, StateRow};
use crate::parquet_files::{FileWriter, ParquetConfig};
use crate::source::{PartitionId, Position};
use crate::test_support::{RecordingStore, TestClock};

mod blooms;
mod restart;
mod seal;

const FIRST_FLUSH: &str = "2026-09-27T10:00:00Z";
const FIRST_NNNN: u64 = 1_790_503_200_000;
const SECOND_FLUSH: &str = "2026-09-27T10:10:00Z";

fn writer_id() -> WriterId {
    "writer-1".parse().unwrap()
}

async fn open_flusher(
    store: &Arc<RecordingStore>,
    clock: &TestClock,
) -> Result<Flusher<TestClock>, FlushError> {
    Flusher::open(
        Arc::clone(store) as Arc<dyn ObjectStore>,
        FileWriter::new(ParquetConfig::default()).unwrap(),
        clock.clone(),
        writer_id(),
        blooms(),
    )
    .await
}

/// Small filters, so that the tests store kilobytes, not megabytes.
fn blooms() -> BloomConfig {
    BloomConfig {
        min_bytes: 64,
        ..BloomConfig::default()
    }
}

async fn flusher(store: &Arc<RecordingStore>, clock: &TestClock) -> Flusher<TestClock> {
    open_flusher(store, clock).await.unwrap()
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
    let bytes = read(store, &format!("{day}/manifest")).await?;
    Some(DayManifest::decode(&bytes).unwrap())
}

async fn stored_checkpoint(store: &RecordingStore) -> Option<Checkpoint> {
    let bytes = read(store, "checkpoint/writer-1.json").await?;
    Some(Checkpoint::from_json(&bytes).unwrap())
}

#[tokio::test]
async fn writes_files_blooms_manifest_and_checkpoint_in_order() {
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
            "20260927/refs.bloom".to_string(),
            "20260927/trees.bloom".to_string(),
            "checkpoint/writer-1.json".to_string(),
            "20260927/manifest".to_string(),
            "checkpoint/writer-1.json".to_string(),
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
        pending: None,
        unsealed: Vec::new(),
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

/// The filters are written too: the day has them from its first flush, thus
/// the flushes after it add to them.
#[tokio::test]
async fn empty_batch_writes_both_files_both_filters_and_an_entry_without_bounds() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let entry = flusher(&store, &clock)
        .await
        .flush(Arc::default())
        .await
        .unwrap();
    assert_eq!(store.writes().len(), 7);
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

    // The first checkpoint write, naming the flush as pending, goes through.
    store.fail_writes_to_after(".json", 1);
    clock.set(SECOND_FLUSH);
    let failed = flusher.flush(batch(positions(&[(0, 20)]))).await;
    assert!(
        matches!(&failed, Err(FlushError::Unfinished(cause)) if matches!(**cause, FlushError::Store(_))),
        "{failed:?}"
    );
    let stored = stored_checkpoint(&store).await.unwrap();
    assert_eq!(stored.positions, before.positions);
    assert_eq!(stored.seq, before.seq);
    assert!(stored.pending.is_some(), "the flush is named as pending");
    assert_eq!(flusher.checkpoint(), &before);
    assert_eq!(manifest(&store, "20260927").await.unwrap().entries.len(), 2);
}

#[tokio::test]
async fn failure_at_the_manifest_or_later_needs_a_restart() {
    for (failing, skip) in [("/manifest", 0), (".json", 1)] {
        let store = Arc::new(RecordingStore::default());
        let clock = TestClock::at(FIRST_FLUSH);
        let mut flusher = flusher(&store, &clock).await;
        store.fail_writes_to_after(failing, skip);
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
async fn failure_naming_the_flush_pending_leaves_no_manifest_and_can_be_retried() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    store.fail_writes_to(Some(".json"));
    assert!(matches!(
        flusher.flush(batch(positions(&[(0, 10)]))).await,
        Err(FlushError::Store(_))
    ));
    assert_eq!(manifest(&store, "20260927").await, None);

    store.fail_writes_to(None);
    clock.set(SECOND_FLUSH);
    let entry = flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();
    assert_eq!(entry.seq, 1);
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
            &Path::from("checkpoint/writer-1.json"),
            b"not json".to_vec().into(),
        )
        .await
        .unwrap();
    let opened = open_flusher(&store, &TestClock::at(FIRST_FLUSH)).await;
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
            &Path::from("20260927/manifest"),
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
    let mut flusher = flusher(&store, &TestClock::at("1969-12-31T23:59:59Z")).await;
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
            &Path::from("20260927/manifest"),
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
async fn flushes_within_a_day_read_nothing() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    let reads_after_first = store.reads().len();
    for (minute, position) in [(10, 2), (20, 3), (30, 4)] {
        clock.set(&format!("2026-09-27T10:{minute:02}:00Z"));
        flusher
            .flush(batch(positions(&[(0, position)])))
            .await
            .unwrap();
    }
    assert_eq!(
        store.reads().len(),
        reads_after_first,
        "later flushes read nothing"
    );
}
