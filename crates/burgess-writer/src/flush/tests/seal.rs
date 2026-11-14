//! Days are sealed once the next day holds a stored flush.

use super::*;

async fn sealed(store: &RecordingStore, day: &str) -> bool {
    manifest(store, day).await.unwrap().sealed
}

fn manifest_writes(store: &RecordingStore, day: &str) -> usize {
    let path = format!("{day}/manifest");
    store
        .writes()
        .iter()
        .filter(|write| **write == path)
        .count()
}

#[tokio::test]
async fn seals_at_the_second_flush_of_the_next_day() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-27T23:50:00Z");
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();

    clock.set("2026-09-28T00:00:00Z");
    flusher.flush(batch(positions(&[(0, 2)]))).await.unwrap();
    assert!(!sealed(&store, "20260927").await, "not at the first flush");

    clock.set("2026-09-28T00:10:00Z");
    flusher.flush(batch(positions(&[(0, 3)]))).await.unwrap();
    assert!(sealed(&store, "20260927").await);
    assert_eq!(manifest(&store, "20260927").await.unwrap().entries.len(), 1);
}

#[tokio::test]
async fn sealed_manifest_is_never_written_again() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-27T23:50:00Z");
    let mut flusher = flusher(&store, &clock).await;
    for (time, position) in [
        ("2026-09-27T23:50:00Z", 1),
        ("2026-09-28T00:00:00Z", 2),
        ("2026-09-28T00:10:00Z", 3),
    ] {
        clock.set(time);
        flusher
            .flush(batch(positions(&[(0, position)])))
            .await
            .unwrap();
    }
    let writes = manifest_writes(&store, "20260927");

    clock.set("2026-09-28T00:20:00Z");
    flusher.flush(batch(positions(&[(0, 4)]))).await.unwrap();
    let mut reopened = self::flusher(&store, &clock).await;
    clock.set("2026-09-28T00:30:00Z");
    reopened.flush(batch(positions(&[(0, 5)]))).await.unwrap();
    assert_eq!(manifest_writes(&store, "20260927"), writes);
}

#[tokio::test]
async fn restart_between_the_two_flushes_still_seals() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-27T23:50:00Z");
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    clock.set("2026-09-28T00:00:00Z");
    flusher.flush(batch(positions(&[(0, 2)]))).await.unwrap();

    clock.set("2026-09-28T00:10:00Z");
    let mut reopened = self::flusher(&store, &clock).await;
    reopened.flush(batch(positions(&[(0, 3)]))).await.unwrap();
    assert!(sealed(&store, "20260927").await);
}

#[tokio::test]
async fn seals_the_last_flush_day_after_a_long_outage() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-20T12:00:00Z");
    flusher(&store, &clock)
        .await
        .flush(batch(positions(&[(0, 1)])))
        .await
        .unwrap();

    clock.set(FIRST_FLUSH);
    let mut reopened = self::flusher(&store, &clock).await;
    reopened.flush(batch(positions(&[(0, 2)]))).await.unwrap();
    assert!(!sealed(&store, "20260920").await);
    clock.set(SECOND_FLUSH);
    reopened.flush(batch(positions(&[(0, 3)]))).await.unwrap();
    assert!(sealed(&store, "20260920").await);
}

#[tokio::test]
async fn failed_seal_writes_no_data_and_can_be_retried() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-27T23:50:00Z");
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    clock.set("2026-09-28T00:00:00Z");
    flusher.flush(batch(positions(&[(0, 2)]))).await.unwrap();

    clock.set("2026-09-28T00:10:00Z");
    store.fail_writes_to(Some("20260927/manifest"));
    let writes = store.writes().len();
    assert!(matches!(
        flusher.flush(batch(positions(&[(0, 3)]))).await,
        Err(FlushError::Store(_))
    ));
    assert_eq!(store.writes().len(), writes, "no data written");

    store.fail_writes_to(None);
    flusher.flush(batch(positions(&[(0, 3)]))).await.unwrap();
    assert!(sealed(&store, "20260927").await);
    assert_eq!(manifest(&store, "20260928").await.unwrap().entries.len(), 2);
}

#[tokio::test]
async fn days_without_flushes_have_nothing_to_seal() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    clock.set(SECOND_FLUSH);
    flusher.flush(batch(positions(&[(0, 2)]))).await.unwrap();
    assert_eq!(manifest(&store, "20260926").await, None);
}

#[tokio::test]
async fn a_day_without_flushes_never_gets_a_manifest() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-27T23:59:00Z");
    let mut flusher = flusher(&store, &clock).await;
    clock.set("2026-09-28T00:01:00Z");
    flusher.flush(batch(positions(&[(0, 1)]))).await.unwrap();
    clock.set("2026-09-28T00:11:00Z");
    flusher.flush(batch(positions(&[(0, 2)]))).await.unwrap();
    assert_eq!(manifest(&store, "20260927").await, None);
}

#[tokio::test]
async fn a_writer_flushing_once_a_day_seals_across_restarts() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at("2026-09-25T23:50:00Z");
    flusher(&store, &clock)
        .await
        .flush(batch(positions(&[(0, 1)])))
        .await
        .unwrap();
    clock.set("2026-09-26T00:10:00Z");
    flusher(&store, &clock)
        .await
        .flush(batch(positions(&[(0, 2)])))
        .await
        .unwrap();

    clock.set("2026-09-28T10:00:00Z");
    let mut reopened = self::flusher(&store, &clock).await;
    reopened.flush(batch(positions(&[(0, 3)]))).await.unwrap();
    clock.set("2026-09-28T10:10:00Z");
    reopened.flush(batch(positions(&[(0, 4)]))).await.unwrap();
    assert!(sealed(&store, "20260925").await);
    assert!(sealed(&store, "20260926").await);
    assert_eq!(stored_checkpoint(&store).await.unwrap().unsealed, []);
}
