//! The bloom filters of a day hold the keys of every file that its manifest
//! lists: after each flush, after a failed one, and after a restart.

use burgess_core::storage::{BloomSize, Day, ObjectName, RefsBloom, RefsKey, TreeKey, TreesBloom};
use object_store::path::Path;

use super::*;

const DAY: &str = "20260927";

/// A batch of `refs` (type, outside id) and of states of `trees`.
fn batch_of(refs: &[(&str, &str)], trees: &[u8]) -> Arc<SortedBatch> {
    let tree_rows = trees.iter().map(|tree| StateRow {
        tree: TreeId::from([*tree; 12]),
        subflow: Subflow::ROOT,
        time: UnixMillis::from(100),
        service: "checkout".into(),
        category: "payment".into(),
        entry: StateEntry::State {
            meta: BTreeMap::new(),
            payload_type: PayloadType::Json,
            payload: b"{}".to_vec(),
        },
    });
    let ref_rows = refs.iter().map(|(ref_type, domain_id)| RefRow {
        ref_type: (*ref_type).into(),
        domain_id: (*domain_id).into(),
        service: "orders".into(),
        category: None,
        tree: TreeId::from([1; 12]),
        subflow: Subflow::ROOT,
        time: UnixMillis::from(100),
    });
    Arc::new(SortedBatch {
        states: tree_rows.collect(),
        refs: ref_rows.collect(),
        positions: positions(&[(0, 10)]),
        skipped: 0,
    })
}

async fn refs_bloom(store: &RecordingStore, day: &str) -> Option<RefsBloom> {
    let bytes = read(store, &format!("{day}/refs.bloom")).await?;
    Some(RefsBloom::decode(&bytes).unwrap())
}

async fn trees_bloom(store: &RecordingStore, day: &str) -> Option<TreesBloom> {
    let bytes = read(store, &format!("{day}/trees.bloom")).await?;
    Some(TreesBloom::decode(&bytes).unwrap())
}

/// Whether the filters of `day` hold the keys of the flush `nnnn` of
/// `refs` and `trees`.
async fn covers(
    store: &RecordingStore,
    day: &str,
    nnnn: UnixMillis,
    refs: &[(&str, &str)],
    trees: &[u8],
) -> bool {
    let refs_bloom = refs_bloom(store, day).await.expect("a refs filter");
    let trees_bloom = trees_bloom(store, day).await.expect("a trees filter");
    let refs_held = refs.iter().all(|(ref_type, domain_id)| {
        refs_bloom.may_contain(&RefsKey::day(ref_type, domain_id))
            && refs_bloom.may_contain(&RefsKey::file(nnnn, ref_type, domain_id))
    });
    let trees_held = trees
        .iter()
        .all(|tree| trees_bloom.may_contain(&TreeKey::file(nnnn, TreeId::from([*tree; 12]))));
    refs_held && trees_held
}

const FIRST: (&[(&str, &str)], &[u8]) = (&[("order_id", "ORD-1"), ("order_id", "ORD-2")], &[1, 2]);
const SECOND: (&[(&str, &str)], &[u8]) = (&[("invoice", "INV-9")], &[3]);

#[tokio::test]
async fn the_filters_hold_the_keys_of_each_listed_file() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;

    let first = flusher.flush(batch_of(FIRST.0, FIRST.1)).await.unwrap();
    clock.set(SECOND_FLUSH);
    let second = flusher.flush(batch_of(SECOND.0, SECOND.1)).await.unwrap();

    assert!(covers(&store, DAY, first.nnnn, FIRST.0, FIRST.1).await);
    assert!(covers(&store, DAY, second.nnnn, SECOND.0, SECOND.1).await);
    // The keys of one file are not the keys of the other.
    let trees = trees_bloom(&store, DAY).await.unwrap();
    assert!(!trees.may_contain(&TreeKey::file(second.nnnn, TreeId::from([1; 12]))));
}

#[tokio::test]
async fn a_flush_onto_a_new_day_starts_new_filters() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch_of(FIRST.0, FIRST.1)).await.unwrap();

    clock.set("2026-09-28T00:05:00Z");
    let next_day = flusher.flush(batch_of(SECOND.0, SECOND.1)).await.unwrap();

    assert!(covers(&store, "20260928", next_day.nnnn, SECOND.0, SECOND.1).await);
    let refs = refs_bloom(&store, "20260928").await.unwrap();
    assert!(!refs.may_contain(&RefsKey::day("order_id", "ORD-1")));
    assert_eq!(
        refs.insertions(),
        2,
        "the day and file keys of one reference"
    );
}

/// A filter is written before the manifest, thus a flush that fails on it
/// lists nothing, and is flushed again.
#[tokio::test]
async fn a_flush_that_fails_on_a_filter_lists_nothing_and_flushes_again() {
    for filter in ["refs.bloom", "trees.bloom"] {
        let store = Arc::new(RecordingStore::default());
        let clock = TestClock::at(FIRST_FLUSH);
        let mut flusher = flusher(&store, &clock).await;

        store.fail_writes_to(Some(filter));
        let failed = flusher.flush(batch_of(FIRST.0, FIRST.1)).await;
        assert!(
            matches!(failed, Err(FlushError::Store(_))),
            "{filter}: {failed:?}"
        );
        assert_eq!(manifest(&store, DAY).await, None, "{filter}");
        store.fail_writes_to(None);

        clock.set(SECOND_FLUSH);
        let retried = flusher.flush(batch_of(FIRST.0, FIRST.1)).await.unwrap();
        assert!(
            covers(&store, DAY, retried.nnnn, FIRST.0, FIRST.1).await,
            "{filter}"
        );
    }
}

/// The filters taken for a failed flush are read again: those the flusher
/// held do not have the keys that the failed flush may have stored.
#[tokio::test]
async fn a_failed_flush_does_not_lose_the_keys_of_the_flushes_before() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    let first = flusher.flush(batch_of(FIRST.0, FIRST.1)).await.unwrap();

    clock.set(SECOND_FLUSH);
    store.fail_writes_to(Some("trees.bloom"));
    assert!(flusher.flush(batch_of(SECOND.0, SECOND.1)).await.is_err());
    store.fail_writes_to(None);
    clock.set("2026-09-27T10:20:00Z");
    let third = flusher.flush(batch_of(SECOND.0, SECOND.1)).await.unwrap();

    assert!(covers(&store, DAY, first.nnnn, FIRST.0, FIRST.1).await);
    assert!(covers(&store, DAY, third.nnnn, SECOND.0, SECOND.1).await);
}

#[tokio::test]
async fn a_restart_keeps_the_keys_of_the_flushes_before() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let first = flusher(&store, &clock)
        .await
        .flush(batch_of(FIRST.0, FIRST.1))
        .await
        .unwrap();

    clock.set(SECOND_FLUSH);
    let second = flusher(&store, &clock)
        .await
        .flush(batch_of(SECOND.0, SECOND.1))
        .await
        .unwrap();

    assert!(covers(&store, DAY, first.nnnn, FIRST.0, FIRST.1).await);
    assert!(covers(&store, DAY, second.nnnn, SECOND.0, SECOND.1).await);
    assert_eq!(refs_bloom(&store, DAY).await.unwrap().insertions(), 6);
    assert_eq!(trees_bloom(&store, DAY).await.unwrap().insertions(), 3);
}

/// A restart drops the flush that the checkpoint does not cover, and the
/// writer stores its records again; the filters cover the files listed
/// then.
#[tokio::test]
async fn a_restart_after_a_dropped_flush_covers_the_files_listed_after_it() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    let first = flusher.flush(batch_of(FIRST.0, FIRST.1)).await.unwrap();
    clock.set(SECOND_FLUSH);
    store.fail_writes_to_after(".json", 1);
    assert!(flusher.flush(batch_of(SECOND.0, SECOND.1)).await.is_err());
    store.fail_writes_to(None);

    clock.set("2026-09-27T10:20:00Z");
    let again = self::flusher(&store, &clock)
        .await
        .flush(batch_of(SECOND.0, SECOND.1))
        .await
        .unwrap();

    let listed = manifest(&store, DAY).await.unwrap().entries;
    assert_eq!(
        listed.iter().map(|entry| entry.nnnn).collect::<Vec<_>>(),
        [first.nnnn, again.nnnn]
    );
    assert!(covers(&store, DAY, first.nnnn, FIRST.0, FIRST.1).await);
    assert!(covers(&store, DAY, again.nnnn, SECOND.0, SECOND.1).await);
}

/// A new filter would not hold the keys of the files listed before it, and
/// a lookup could then miss them. A lookup reads each file of a day without
/// a filter instead.
#[tokio::test]
async fn a_day_whose_filter_is_gone_gets_none_and_keeps_the_other() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let first = flusher(&store, &clock)
        .await
        .flush(batch_of(FIRST.0, FIRST.1))
        .await
        .unwrap();
    store
        .inner
        .delete(&Path::from(format!("{DAY}/refs.bloom")))
        .await
        .unwrap();

    clock.set(SECOND_FLUSH);
    let mut reopened = flusher(&store, &clock).await;
    let second = reopened.flush(batch_of(SECOND.0, SECOND.1)).await.unwrap();
    clock.set("2026-09-27T10:20:00Z");
    reopened.flush(batch_of(SECOND.0, SECOND.1)).await.unwrap();

    assert!(refs_bloom(&store, DAY).await.is_none());
    let trees = trees_bloom(&store, DAY).await.unwrap();
    assert!(trees.may_contain(&TreeKey::file(first.nnnn, TreeId::from([1; 12]))));
    assert!(trees.may_contain(&TreeKey::file(second.nnnn, TreeId::from([3; 12]))));
}

#[tokio::test]
async fn a_day_whose_filter_cannot_be_read_gets_none() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch_of(FIRST.0, FIRST.1))
        .await
        .unwrap();
    let damaged = Path::from(format!("{DAY}/trees.bloom"));
    store
        .inner
        .put(&damaged, b"BGSC damaged".to_vec().into())
        .await
        .unwrap();

    clock.set(SECOND_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch_of(SECOND.0, SECOND.1))
        .await
        .unwrap();

    assert_eq!(read(&store, &format!("{DAY}/trees.bloom")).await, None);
    assert!(refs_bloom(&store, DAY).await.is_some());
}

/// A filter that this writer cannot extend stays readable to another
/// reader, which would trust it for the files listed after it. Thus it is
/// deleted.
#[tokio::test]
async fn a_filter_of_another_writer_is_deleted_not_extended() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch_of(FIRST.0, FIRST.1))
        .await
        .unwrap();
    let foreign = RefsBloom::new(
        DAY.parse().unwrap(),
        "writer-2".parse().unwrap(),
        BloomSize {
            capacity: 10.try_into().unwrap(),
            target_fpp: BloomConfig::default().target_fpp,
            min_bytes: 0,
        },
    )
    .unwrap();
    let name = Path::from(format!("{DAY}/refs.bloom"));
    store
        .inner
        .put(&name, foreign.encode().into())
        .await
        .unwrap();

    clock.set(SECOND_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch_of(SECOND.0, SECOND.1))
        .await
        .unwrap();

    assert!(refs_bloom(&store, DAY).await.is_none());
    assert!(trees_bloom(&store, DAY).await.is_some());
}

/// Each try of a flush that fails after it stored its filters starts from
/// the filters without its keys. Without that, the tries of a long outage
/// fill the filters with the keys of files that are never listed.
#[tokio::test]
async fn the_tries_of_a_failing_flush_add_its_keys_one_time() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch_of(FIRST.0, FIRST.1)).await.unwrap();

    store.fail_writes_to(Some(".json"));
    for minute in 1..=3 {
        clock.set(&format!("2026-09-27T10:0{minute}:00Z"));
        let failed = flusher.flush(batch_of(SECOND.0, SECOND.1)).await;
        assert!(matches!(failed, Err(FlushError::Store(_))), "{failed:?}");
    }
    store.fail_writes_to(None);
    clock.set(SECOND_FLUSH);
    flusher.flush(batch_of(SECOND.0, SECOND.1)).await.unwrap();

    assert_eq!(refs_bloom(&store, DAY).await.unwrap().insertions(), 6);
    assert_eq!(trees_bloom(&store, DAY).await.unwrap().insertions(), 3);
}

/// A filter that the batch adds nothing to is as stored.
#[tokio::test]
async fn a_filter_without_new_keys_is_not_written_again() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let mut flusher = flusher(&store, &clock).await;
    flusher.flush(batch_of(FIRST.0, FIRST.1)).await.unwrap();
    let before = store.writes().len();

    clock.set(SECOND_FLUSH);
    let trees_only = flusher.flush(batch_of(&[], &[7])).await.unwrap();

    let written = &store.writes()[before..];
    assert!(
        written.contains(&format!("{DAY}/trees.bloom")),
        "{written:?}"
    );
    assert!(
        !written.contains(&format!("{DAY}/refs.bloom")),
        "{written:?}"
    );
    assert!(covers(&store, DAY, trees_only.nnnn, &[], &[7]).await);
}

/// A size that no filter can have would fail only at the first flush of
/// the next day.
#[tokio::test]
async fn a_flusher_refuses_bloom_settings_that_cannot_size_a_filter() {
    let store = Arc::new(RecordingStore::default());
    let opened = Flusher::open(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        FileWriter::new(ParquetConfig::default()).unwrap(),
        TestClock::at(FIRST_FLUSH),
        writer_id(),
        BloomConfig {
            min_bytes: u64::MAX,
            ..BloomConfig::default()
        },
    )
    .await;

    assert!(matches!(opened, Err(FlushError::BloomConfig(_))));
}

/// The counts of a day size the filters of the next days, thus a key is
/// counted once for each flush. References that differ only in the case of
/// ASCII letters are one key.
#[tokio::test]
async fn a_key_is_added_once_for_each_flush() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    let refs = [
        ("order_id", "ORD-1"),
        ("order_id", "ORD-1"),
        ("order_id", "ord-1"),
        ("ORDER_ID", "ORD-1"),
    ];
    flusher(&store, &clock)
        .await
        .flush(batch_of(&refs, &[1, 1, 1]))
        .await
        .unwrap();

    assert_eq!(refs_bloom(&store, DAY).await.unwrap().insertions(), 2);
    assert_eq!(trees_bloom(&store, DAY).await.unwrap().insertions(), 1);
}

/// `keys` keys inserted in a refs filter stored for `day`.
async fn store_refs_filter(store: &RecordingStore, day: &str, keys: u64) {
    let day: Day = day.parse().unwrap();
    let mut filter = RefsBloom::new(
        day,
        writer_id(),
        BloomSize {
            capacity: keys.max(1).try_into().unwrap(),
            target_fpp: BloomConfig::default().target_fpp,
            min_bytes: 0,
        },
    )
    .unwrap();
    for n in 0..keys {
        filter.insert(&RefsKey::day("order_id", &format!("ORD-{n}")));
    }
    let name = ObjectName::RefsBloom { day };
    store
        .inner
        .put(&Path::from(name.to_string()), filter.encode().into())
        .await
        .unwrap();
}

/// The busiest of the days in the window sizes the filters of a new day,
/// with room to grow; a day before the window and an unreadable filter do
/// not.
#[tokio::test]
async fn a_new_day_is_sized_by_the_busiest_day_of_the_window() {
    let store = Arc::new(RecordingStore::default());
    store_refs_filter(&store, "20260923", 9_000).await;
    store_refs_filter(&store, "20260924", 1_000).await;
    store_refs_filter(&store, "20260925", 3_000).await;
    store
        .inner
        .put(
            &Path::from("20260926/refs.bloom"),
            b"damaged".to_vec().into(),
        )
        .await
        .unwrap();
    let clock = TestClock::at(FIRST_FLUSH);

    flusher(&store, &clock)
        .await
        .flush(batch_of(FIRST.0, FIRST.1))
        .await
        .unwrap();

    let refs = refs_bloom(&store, DAY).await.unwrap();
    assert_eq!(refs.capacity().get(), 4_500, "3,000 keys times 1.5");
    // No trees filter before: the floor sizes it.
    let trees = read(&store, &format!("{DAY}/trees.bloom")).await.unwrap();
    let floor = blooms().min_bytes as usize;
    assert!(
        trees.len() > floor && trees.len() < floor + 100,
        "{}",
        trees.len()
    );
}

#[tokio::test]
async fn a_new_filter_has_at_least_the_floor_size() {
    let store = Arc::new(RecordingStore::default());
    let clock = TestClock::at(FIRST_FLUSH);
    flusher(&store, &clock)
        .await
        .flush(batch_of(FIRST.0, FIRST.1))
        .await
        .unwrap();

    let bytes = read(&store, &format!("{DAY}/refs.bloom")).await.unwrap();
    let filter_bytes = blooms().min_bytes as usize;
    assert!(
        bytes.len() > filter_bytes && bytes.len() < filter_bytes + 100,
        "{}",
        bytes.len()
    );
}
