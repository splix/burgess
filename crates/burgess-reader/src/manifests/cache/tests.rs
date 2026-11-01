use std::num::NonZeroU32;
use std::time::Duration;

use burgess_core::flow::TreeId;
use burgess_core::storage::{FileStats, FlushEntry, ManifestError, StatesBounds};
use burgess_core::wire::UnixMillis;
use object_store::memory::InMemory;

use super::*;
use crate::manifests::FlushGroup;

const TTL: Duration = Duration::from_secs(5);
const RETENTION_DAYS: u32 = 30;

/// Today, so that the day stays within retention whenever the tests run.
fn day() -> Day {
    days_ago(0)
}

fn days_ago(days: i64) -> Day {
    let today = Date::from(Day::utc(Timestamp::now()).unwrap());
    Day::try_from(today.checked_sub(Span::new().days(days)).unwrap()).unwrap()
}

fn entry(seq: u64) -> FlushEntry {
    FlushEntry {
        nnnn: UnixMillis::from(1_793_000_000_000 + seq),
        seq,
        states_bounds: Some(StatesBounds {
            min_tree: TreeId::from([1; 12]),
            max_tree: TreeId::from([2; 12]),
            min_ts: UnixMillis::from(1_793_000_000_000),
            max_ts: UnixMillis::from(1_793_000_000_000),
        }),
        states: FileStats {
            rows: 3,
            bytes: 100,
        },
        refs: FileStats { rows: 1, bytes: 50 },
    }
}

fn manifest(day: Day, sealed: bool, seqs: &[u64]) -> DayManifest {
    DayManifest {
        sealed,
        entries: seqs.iter().copied().map(entry).collect(),
        ..DayManifest::new(day, "writer-1".parse().unwrap())
    }
}

async fn put(store: &InMemory, name: &ObjectName, bytes: Vec<u8>) {
    store
        .put(&Path::from(name.to_string()), bytes.into())
        .await
        .unwrap();
}

async fn store_manifest(store: &InMemory, manifest: &DayManifest) {
    let name = ObjectName::Manifest { day: manifest.day };
    put(store, &name, manifest.encode().unwrap()).await;
}

async fn delete_manifest(store: &InMemory) {
    let name = ObjectName::Manifest { day: day() };
    store.delete(&Path::from(name.to_string())).await.unwrap();
}

fn config(unsealed_ttl: Duration) -> ManifestCacheConfig {
    ManifestCacheConfig {
        unsealed_ttl,
        retention_days: NonZeroU32::new(RETENTION_DAYS).unwrap(),
    }
}

fn cache(store: &Arc<InMemory>) -> ManifestCache {
    ManifestCache::new(store.clone(), config(TTL))
}

/// The seqs of the groups the cache lists for the day.
async fn seqs(cache: &ManifestCache) -> Option<Vec<u64>> {
    let flushes = cache.day(day()).await.unwrap()?;
    Some(flushes.groups().iter().map(FlushGroup::seq).collect())
}

#[tokio::test]
async fn missing_manifest_is_a_negative_day() {
    let store = Arc::new(InMemory::new());
    assert_eq!(cache(&store).day(day()).await.unwrap(), None);
}

#[tokio::test]
async fn groups_are_the_listed_flushes_in_order() {
    let store = Arc::new(InMemory::new());
    store_manifest(&store, &manifest(day(), false, &[1, 2])).await;
    let flushes = cache(&store).day(day()).await.unwrap().unwrap();
    let files: Vec<(String, String)> = flushes
        .groups()
        .iter()
        .map(|group| {
            (
                group.states_file().to_string(),
                group.refs_file().to_string(),
            )
        })
        .collect();
    let day = day();
    assert_eq!(
        files,
        [
            (
                format!("{day}/1793000000001-states.parquet"),
                format!("{day}/1793000000001-refs.parquet")
            ),
            (
                format!("{day}/1793000000002-states.parquet"),
                format!("{day}/1793000000002-refs.parquet")
            ),
        ]
    );
    assert_eq!(flushes.groups()[0].states(), entry(1).states);
}

#[tokio::test]
async fn files_the_manifest_does_not_list_are_not_visible() {
    let store = Arc::new(InMemory::new());
    store_manifest(&store, &manifest(day(), false, &[1])).await;
    // A flush that stopped after its states file, and one that stopped
    // before adding itself to the manifest.
    let unfinished = UnixMillis::from(1_793_000_000_100);
    let unlisted = UnixMillis::from(1_793_000_000_200);
    for name in [
        ObjectName::StatesFile {
            day: day(),
            nnnn: unfinished,
        },
        ObjectName::StatesFile {
            day: day(),
            nnnn: unlisted,
        },
        ObjectName::RefsFile {
            day: day(),
            nnnn: unlisted,
        },
    ] {
        put(&store, &name, b"PAR1".to_vec()).await;
    }
    assert_eq!(seqs(&cache(&store)).await, Some(vec![1]));
}

#[tokio::test(start_paused = true)]
async fn sealed_manifest_is_fetched_once() {
    let store = Arc::new(InMemory::new());
    store_manifest(&store, &manifest(day(), true, &[1])).await;
    let cache = cache(&store);
    assert_eq!(seqs(&cache).await, Some(vec![1]));
    delete_manifest(&store).await;
    tokio::time::advance(TTL * 100).await;
    assert_eq!(seqs(&cache).await, Some(vec![1]));
}

#[tokio::test(start_paused = true)]
async fn unsealed_manifest_is_reused_until_the_ttl() {
    let store = Arc::new(InMemory::new());
    store_manifest(&store, &manifest(day(), false, &[1])).await;
    let cache = cache(&store);
    assert_eq!(seqs(&cache).await, Some(vec![1]));

    store_manifest(&store, &manifest(day(), false, &[1, 2])).await;
    tokio::time::advance(TTL - Duration::from_millis(1)).await;
    assert_eq!(seqs(&cache).await, Some(vec![1]));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(seqs(&cache).await, Some(vec![1, 2]));
}

#[tokio::test(start_paused = true)]
async fn refetched_manifest_replaces_the_old_one_whole() {
    let store = Arc::new(InMemory::new());
    store_manifest(&store, &manifest(day(), false, &[1, 2, 3])).await;
    let cache = cache(&store);
    assert_eq!(seqs(&cache).await, Some(vec![1, 2, 3]));

    // A writer restart dropped the flushes its checkpoint did not cover.
    store_manifest(&store, &manifest(day(), false, &[1])).await;
    tokio::time::advance(TTL).await;
    assert_eq!(seqs(&cache).await, Some(vec![1]));
}

#[tokio::test(start_paused = true)]
async fn manifest_sealed_since_is_kept_for_good() {
    let store = Arc::new(InMemory::new());
    store_manifest(&store, &manifest(day(), false, &[1])).await;
    let cache = cache(&store);
    seqs(&cache).await;

    store_manifest(&store, &manifest(day(), true, &[1, 2])).await;
    tokio::time::advance(TTL).await;
    assert_eq!(seqs(&cache).await, Some(vec![1, 2]));
    delete_manifest(&store).await;
    tokio::time::advance(TTL * 100).await;
    assert_eq!(seqs(&cache).await, Some(vec![1, 2]));
}

#[tokio::test(start_paused = true)]
async fn negative_day_is_checked_again_after_the_ttl() {
    let store = Arc::new(InMemory::new());
    let cache = cache(&store);
    assert_eq!(seqs(&cache).await, None);

    store_manifest(&store, &manifest(day(), false, &[1])).await;
    assert_eq!(seqs(&cache).await, None);
    tokio::time::advance(TTL).await;
    assert_eq!(seqs(&cache).await, Some(vec![1]));
}

#[tokio::test]
async fn zero_ttl_fetches_every_time() {
    let store = Arc::new(InMemory::new());
    store_manifest(&store, &manifest(day(), false, &[1])).await;
    let cache = ManifestCache::new(store.clone(), config(Duration::ZERO));
    seqs(&cache).await;
    store_manifest(&store, &manifest(day(), false, &[1, 2])).await;
    assert_eq!(seqs(&cache).await, Some(vec![1, 2]));
}

#[tokio::test]
async fn undecodable_manifest_is_an_error() {
    let store = Arc::new(InMemory::new());
    put(
        &store,
        &ObjectName::Manifest { day: day() },
        b"garbage".to_vec(),
    )
    .await;
    let error = cache(&store).day(day()).await.unwrap_err();
    assert!(
        matches!(
            error,
            ManifestReadError::Invalid {
                source: ManifestError::Frame(_),
                ..
            }
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn manifest_of_another_day_is_an_error() {
    let store = Arc::new(InMemory::new());
    let other = days_ago(1);
    let misplaced = manifest(other, true, &[1]);
    put(
        &store,
        &ObjectName::Manifest { day: day() },
        misplaced.encode().unwrap(),
    )
    .await;
    let error = cache(&store).day(day()).await.unwrap_err();
    assert!(
        matches!(error, ManifestReadError::WrongDay { found, .. } if found == other),
        "{error:?}"
    );
}

#[tokio::test]
async fn days_before_retention_are_empty_without_asking_the_store() {
    let store = Arc::new(InMemory::new());
    let expired = days_ago(i64::from(RETENTION_DAYS) + 1);
    store_manifest(&store, &manifest(expired, true, &[1])).await;
    assert_eq!(cache(&store).day(expired).await.unwrap(), None);
}

#[tokio::test]
async fn last_day_of_retention_is_still_read() {
    let store = Arc::new(InMemory::new());
    let oldest = days_ago(i64::from(RETENTION_DAYS));
    store_manifest(&store, &manifest(oldest, true, &[1])).await;
    assert!(cache(&store).day(oldest).await.unwrap().is_some());
}

fn cached(sealed: bool, seqs: &[u64], fetched_at: Instant) -> Cached {
    Cached {
        flushes: Some(Arc::new(manifest(day(), sealed, seqs).into())),
        fetched_at,
    }
}

fn kept_seqs(kept: Option<Arc<DayFlushes>>) -> Vec<u64> {
    kept.unwrap().groups().iter().map(FlushGroup::seq).collect()
}

#[tokio::test(start_paused = true)]
async fn answer_fetched_earlier_does_not_replace_a_later_one() {
    let store = Arc::new(InMemory::new());
    let cache = cache(&store);
    let slow_started = Instant::now();
    tokio::time::advance(Duration::from_millis(10)).await;
    cache.keep(day(), cached(false, &[1, 2], Instant::now()));

    let kept = cache.keep(day(), cached(false, &[1], slow_started));
    assert_eq!(kept_seqs(kept), [1, 2]);
    assert_eq!(seqs(&cache).await, Some(vec![1, 2]));
}

#[tokio::test(start_paused = true)]
async fn sealed_answer_is_never_replaced() {
    let store = Arc::new(InMemory::new());
    let cache = cache(&store);
    cache.keep(day(), cached(true, &[1, 2], Instant::now()));
    tokio::time::advance(Duration::from_millis(10)).await;

    let kept = cache.keep(day(), cached(false, &[1], Instant::now()));
    assert_eq!(kept_seqs(kept), [1, 2]);
}

#[tokio::test]
async fn expired_days_are_dropped_from_memory() {
    let store = Arc::new(InMemory::new());
    let cache = cache(&store);
    // Cached while it was still within retention.
    let expired = days_ago(i64::from(RETENTION_DAYS) + 1);
    cache.lock().insert(
        expired,
        Cached {
            flushes: None,
            fetched_at: Instant::now(),
        },
    );

    cache.day(day()).await.unwrap();
    assert!(!cache.lock().contains_key(&expired));
}
