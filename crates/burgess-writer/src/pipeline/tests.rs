//! The loop over an in-memory log and store, on paused time.

use std::num::NonZeroUsize;
use std::time::Duration;

use burgess_core::storage::DayManifest;
use burgess_core::validation::Limits;
use burgess_core::wire::{Body, PayloadType, Record, State, UnixMillis};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::task::JoinHandle;

use super::*;
use crate::batch::BatchConfig;
use crate::flush::BloomConfig;
use crate::parquet_files::{FileWriter, ParquetConfig};
use crate::source::{MemoryLog, MemoryLogError, PartitionId, Position};
use crate::test_support::{RecordingStore, TestClock};

const INTERVAL: Duration = Duration::from_secs(600);

type Running = JoinHandle<Result<(), PipelineError<MemoryLogError>>>;

fn log_with(records: u64) -> MemoryLog {
    let log = MemoryLog::new(1);
    for ms in 0..records {
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
    log
}

/// Starts a pipeline the way the writer does: flusher first, then the log
/// resumed from its checkpoint.
async fn start(log: &MemoryLog, store: &Arc<RecordingStore>) -> (watch::Sender<bool>, Running) {
    let flusher = Flusher::open(
        Arc::clone(store) as Arc<dyn ObjectStore>,
        FileWriter::new(ParquetConfig::default()).unwrap(),
        TestClock::at("2026-09-27T10:00:00Z"),
        "writer-1".parse().unwrap(),
        BloomConfig::default(),
    )
    .await
    .unwrap();
    let mut reader = log.reader();
    reader
        .resume(&flusher.checkpoint().positions)
        .await
        .unwrap();
    let config = BatchConfig {
        max_interval: INTERVAL,
        max_bytes: NonZeroUsize::new(usize::MAX).unwrap(),
    };
    let accumulator = Accumulator::new(reader, config, Limits::default()).unwrap();
    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(Pipeline::new(accumulator, flusher).run(stopped));
    (stop, running)
}

async fn stop_after(
    after: Duration,
    stop: watch::Sender<bool>,
    running: Running,
) -> Result<(), PipelineError<MemoryLogError>> {
    tokio::time::sleep(after).await;
    stop.send(true).unwrap();
    running.await.unwrap()
}

async fn manifest(store: &RecordingStore) -> Option<DayManifest> {
    match store.inner.get(&Path::from("20260927/manifest")).await {
        Ok(stored) => Some(DayManifest::decode(&stored.bytes().await.unwrap()).unwrap()),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(error) => panic!("{error}"),
    }
}

fn rows(manifest: &DayManifest) -> Vec<u64> {
    manifest
        .entries
        .iter()
        .map(|entry| entry.states.rows)
        .collect()
}

#[tokio::test(start_paused = true)]
async fn stop_stores_what_was_read_exactly_once() {
    let log = log_with(3);
    let store = Arc::new(RecordingStore::default());
    let (stop, running) = start(&log, &store).await;
    stop_after(Duration::from_secs(60), stop, running)
        .await
        .unwrap();

    assert_eq!(rows(&manifest(&store).await.unwrap()), [3]);
    let reopened = Flusher::open(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        FileWriter::new(ParquetConfig::default()).unwrap(),
        TestClock::at("2026-09-27T11:00:00Z"),
        "writer-1".parse().unwrap(),
        BloomConfig::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened.checkpoint().positions,
        [(PartitionId::from(0), Position::from(3))].into()
    );
}

#[tokio::test(start_paused = true)]
async fn stop_with_nothing_read_stores_nothing() {
    let log = MemoryLog::new(1);
    let store = Arc::new(RecordingStore::default());
    let (stop, running) = start(&log, &store).await;
    stop_after(Duration::from_secs(60), stop, running)
        .await
        .unwrap();
    assert!(store.writes().is_empty());
}

#[tokio::test(start_paused = true)]
async fn stop_after_interval_flushes_adds_no_extra_flush() {
    let log = log_with(3);
    let store = Arc::new(RecordingStore::default());
    let (stop, running) = start(&log, &store).await;
    // Flushes at 10 and 20 minutes; nothing is read after the second.
    stop_after(Duration::from_secs(25 * 60), stop, running)
        .await
        .unwrap();
    assert_eq!(rows(&manifest(&store).await.unwrap()), [3, 0]);
}

#[tokio::test(start_paused = true)]
async fn store_failures_are_retried_until_they_pass() {
    let log = log_with(3);
    let store = Arc::new(RecordingStore::default());
    store.fail_writes_to(Some("-states.parquet"));
    let (stop, running) = start(&log, &store).await;

    let healing = Arc::clone(&store);
    tokio::spawn(async move {
        tokio::time::sleep(INTERVAL + Duration::from_secs(5)).await;
        healing.fail_writes_to(None);
    });
    stop_after(INTERVAL + Duration::from_secs(90), stop, running)
        .await
        .unwrap();
    assert_eq!(rows(&manifest(&store).await.unwrap()), [3]);
}

#[tokio::test(start_paused = true)]
async fn a_failure_after_the_manifest_stops_the_writer_for_a_restart() {
    let log = log_with(3);
    let store = Arc::new(RecordingStore::default());
    store.fail_writes_to(Some("/manifest"));
    let (_stop, running) = start(&log, &store).await;
    let stopped = running.await.unwrap();
    assert!(
        matches!(
            stopped,
            Err(PipelineError::Flush(FlushError::Unfinished(_)))
        ),
        "{stopped:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn stopping_ends_the_retries() {
    let log = log_with(3);
    let store = Arc::new(RecordingStore::default());
    store.fail_writes_to(Some("-states.parquet"));
    let (stop, running) = start(&log, &store).await;
    let stopped = stop_after(INTERVAL + Duration::from_secs(10), stop, running).await;
    assert!(
        matches!(stopped, Err(PipelineError::Flush(FlushError::Store(_)))),
        "{stopped:?}"
    );
    assert_eq!(manifest(&store).await, None);
}
