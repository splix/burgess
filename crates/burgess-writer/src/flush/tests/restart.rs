//! Opening a flusher reconciles the recent manifests with the checkpoint.

use std::num::NonZeroUsize;
use std::time::Duration;

use burgess_core::validation::Limits;
use burgess_core::wire::{Body, Record, State};

use super::*;
use crate::batch::{Accumulator, BatchConfig};
use crate::source::{MemoryLog, PartitionedLog};

fn seqs(manifest: &DayManifest) -> Vec<u64> {
    manifest.entries.iter().map(|entry| entry.seq).collect()
}

/// Leaves a flush listed in the manifest whose final checkpoint never got
/// written; the checkpoint naming it as pending did.
async fn crash_before_checkpoint(flusher: &mut Flusher<TestClock>, store: &RecordingStore) {
    store.fail_writes_to_after(".json", 1);
    assert!(flusher.flush(batch(positions(&[(0, 99)]))).await.is_err());
    store.fail_writes_to(None);
}

#[tokio::test]
async fn drops_the_flush_the_checkpoint_does_not_cover() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();
    clock.set(SECOND_FLUSH);
    crash_before_checkpoint(&mut flusher, &store).await;
    assert_eq!(seqs(&manifest(&store, "20260927").await.unwrap()), [1, 2]);

    let mut reopened = self::flusher(&store, &clock).await;
    assert_eq!(seqs(&manifest(&store, "20260927").await.unwrap()), [1]);
    assert_eq!(reopened.checkpoint().positions, positions(&[(0, 10)]));

    clock.set("2026-09-27T10:20:00Z");
    reopened.flush(batch(positions(&[(0, 99)]))).await.unwrap();
    assert_eq!(seqs(&manifest(&store, "20260927").await.unwrap()), [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn crash_before_the_checkpoint_replays_without_duplicate_rows() {
    let log = MemoryLog::new(1);
    for ms in 0..3 {
        let record = Record {
            flow_id: "0192f3a1b2c3d4e5f6a7b8c900000000".parse().unwrap(),
            time: UnixMillis::from(ms),
            body: Body::State(State {
                service: "checkout".into(),
                payload_type: PayloadType::Json.into(),
                ..State::default()
            }),
        };
        log.append(PartitionId::from(0), record.encode()).unwrap();
    }
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let config = BatchConfig {
        max_interval: Duration::from_secs(60),
        max_bytes: NonZeroUsize::new(usize::MAX).unwrap(),
    };

    // First run: the batch reaches the manifest, but not the checkpoint.
    let mut flusher = flusher(&store, &clock).await;
    let mut reader = log.reader();
    reader
        .resume(&flusher.checkpoint().positions)
        .await
        .unwrap();
    let mut accumulator = Accumulator::new(reader, config.clone(), Limits::default()).unwrap();
    let batch = Arc::new(accumulator.next_batch().await.unwrap());
    store.fail_writes_to_after(".json", 1);
    assert!(flusher.flush(batch).await.is_err());
    store.fail_writes_to(None);

    // Restart: reopen, read again from the checkpoint, flush again.
    clock.set(SECOND_FLUSH);
    let mut flusher = self::flusher(&store, &clock).await;
    let mut reader = log.reader();
    reader
        .resume(&flusher.checkpoint().positions)
        .await
        .unwrap();
    let mut accumulator = Accumulator::new(reader, config, Limits::default()).unwrap();
    let batch = Arc::new(accumulator.next_batch().await.unwrap());
    flusher.flush(batch).await.unwrap();

    let manifest = manifest(&store, "20260927").await.unwrap();
    let visible_rows: u64 = manifest.entries.iter().map(|entry| entry.states.rows).sum();
    assert_eq!(visible_rows, 3, "each record visible once");
    assert_eq!(seqs(&manifest), [1]);
}

#[tokio::test]
async fn fixes_the_pending_flush_on_yesterday() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-27T23:50:00Z");
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();
    clock.set("2026-09-27T23:59:00Z");
    crash_before_checkpoint(&mut flusher, &store).await;

    clock.set("2026-09-28T00:05:00Z");
    self::flusher(&store, &clock).await;
    assert_eq!(seqs(&manifest(&store, "20260927").await.unwrap()), [1]);
}

#[tokio::test]
async fn fixes_the_pending_flush_after_a_long_outage() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-20T12:00:00Z");
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();
    clock.set("2026-09-20T12:10:00Z");
    crash_before_checkpoint(&mut flusher, &store).await;

    clock.set(FIRST_FLUSH);
    self::flusher(&store, &clock).await;
    assert_eq!(seqs(&manifest(&store, "20260920").await.unwrap()), [1]);
}

#[tokio::test]
async fn drops_a_failed_first_flush_without_any_checkpoint() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    crash_before_checkpoint(&mut flusher, &store).await;

    let reopened = self::flusher(&store, &clock).await;
    assert!(
        manifest(&store, "20260927")
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(reopened.checkpoint(), &Checkpoint::default());
}

#[tokio::test]
async fn first_start_reads_and_writes_nothing_else() {
    let store = Arc::new(RecordingStore::default());
    let flusher = flusher(&store, &TestClock::at(FIRST_FLUSH)).await;
    assert_eq!(flusher.checkpoint(), &Checkpoint::default());
    assert!(store.writes().is_empty());
    assert_eq!(store.reads(), ["_checkpoint/writer-1.json"]);
}

#[tokio::test]
async fn nothing_to_drop_means_nothing_rewritten() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch(positions(&[(0, 10)])))
        .await
        .unwrap();
    let writes = store.writes().len();
    self::flusher(&store, &clock).await;
    assert_eq!(store.writes().len(), writes);
}

#[tokio::test]
async fn sealed_day_with_uncovered_flushes_refuses_to_start() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();
    clock.set(SECOND_FLUSH);
    crash_before_checkpoint(&mut flusher, &store).await;
    let mut sealed = manifest(&store, "20260927").await.unwrap();
    sealed.sealed = true;
    store
        .put(
            &Path::from("20260927/_manifest"),
            sealed.encode().unwrap().into(),
        )
        .await
        .unwrap();

    let writes = store.writes().len();
    let opened = open_flusher(&store, &clock).await;
    assert!(matches!(
        opened,
        Err(FlushError::SealedManifestAhead { .. })
    ));
    assert_eq!(store.writes().len(), writes, "sealed manifest untouched");
    assert_eq!(manifest(&store, "20260927").await, Some(sealed));
}

#[tokio::test]
async fn fixes_a_failed_first_flush_from_long_ago() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-20T12:00:00Z");
    let mut flusher = flusher(&store, &clock).await;
    crash_before_checkpoint(&mut flusher, &store).await;

    clock.set(FIRST_FLUSH);
    let mut reopened = self::flusher(&store, &clock).await;
    assert!(
        manifest(&store, "20260920")
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    reopened.flush(batch(positions(&[(0, 99)]))).await.unwrap();
    assert_eq!(seqs(&manifest(&store, "20260927").await.unwrap()), [1]);
}

#[tokio::test]
async fn fixes_a_pending_flush_on_a_day_ahead_of_the_clock() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 10)]))).await.unwrap();
    clock.set("2026-09-28T10:00:00Z");
    crash_before_checkpoint(&mut flusher, &store).await;
    assert_eq!(seqs(&manifest(&store, "20260928").await.unwrap()), [2]);

    // The clock is corrected back before the restart.
    clock.set(SECOND_FLUSH);
    self::flusher(&store, &clock).await;
    assert!(
        manifest(&store, "20260928")
            .await
            .unwrap()
            .entries
            .is_empty()
    );
}

#[tokio::test]
async fn missing_checkpoint_drops_nothing() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch(positions(&[(0, 10)])))
        .await
        .unwrap();
    store
        .inner
        .delete(&Path::from("_checkpoint/writer-1.json"))
        .await
        .unwrap();

    let writes = store.writes().len();
    let reopened = self::flusher(&store, &clock).await;
    assert_eq!(reopened.checkpoint(), &Checkpoint::default());
    assert_eq!(store.writes().len(), writes);
    assert_eq!(seqs(&manifest(&store, "20260927").await.unwrap()), [1]);
}

#[tokio::test]
async fn foreign_manifest_of_another_day_does_not_block_the_start() {
    let store = Arc::new(RecordingStore::default());
    let other: WriterId = "writer-2".parse().unwrap();
    let foreign = DayManifest::new("20260926".parse().unwrap(), other);
    store
        .put(
            &Path::from("20260926/_manifest"),
            foreign.encode().unwrap().into(),
        )
        .await
        .unwrap();

    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    assert_eq!(seqs(&manifest(&store, "20260927").await.unwrap()), [1]);
}
