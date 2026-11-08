//! States lookups over days of real files: which states, which days and
//! which files.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use burgess_core::flow::TreeId;
use burgess_core::storage::{Day, ObjectName, StateEntry, StateRow};
use burgess_core::wire::UnixMillis;
use jiff::{SignedDuration, Timestamp};
use object_store::ObjectStoreExt;

use super::*;
use crate::day_scan::ScanConfig;
use crate::manifests::{ManifestCache, ManifestCacheConfig};
use crate::ordering::TsOrder;
use crate::states_file::StatesFilter;
use crate::test_support::{Days, around, at, days_ago, minted_on, noon, state_row};

/// A state of `tree` with `meta`, `offset` milliseconds after noon of `day`.
fn state(tree: TreeId, day: Day, offset: u64, meta: &[(&str, &str)]) -> StateRow {
    let mut row = state_row(tree, 0, at(day, offset), None);
    if let StateEntry::State { meta: labels, .. } = &mut row.entry {
        *labels = meta
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
    }
    row
}

/// The states of the rows of [`state`], from the noon of `since` to the
/// noon of `until`.
fn filter(since: Day, until: Day) -> StatesFilter {
    StatesFilter {
        service: "checkout".into(),
        category: "step".into(),
        meta: BTreeMap::new(),
        since: noon(since),
        until: noon(until),
        payload: None,
    }
}

fn wanted(rows: usize) -> NonZeroUsize {
    NonZeroUsize::new(rows).unwrap()
}

async fn states(days: &Days, filter: StatesFilter) -> FoundStates {
    days.states_lookup(ScanConfig::default())
        .states(filter, wanted(100), None)
        .await
}

/// The time of each state, as its offset after noon of `day`.
fn offsets(found: &FoundStates, day: Day) -> Vec<i64> {
    found
        .rows
        .iter()
        .map(|row| u64::from(row.time) as i64 - at(day, 0) as i64)
        .collect()
}

#[tokio::test]
async fn a_state_stored_the_day_after_its_time_is_found() {
    let (day, flushed) = (days_ago(3), days_ago(2));
    let late = state(minted_on(day, 1), day, 11 * 3_600_000, &[]);
    let days = Days::new();
    days.put(flushed, &[std::slice::from_ref(&late)]).await;
    let window = StatesFilter {
        until: noon(day) + SignedDuration::from_mins(11 * 60 + 30),
        ..filter(day, day)
    };

    let found = states(&days, window).await;

    assert_eq!(offsets(&found, day), [11 * 3_600_000]);
    assert!(found.problems.is_empty(), "{:?}", found.problems);
    assert!(
        !days.manifest_days().contains(&days_ago(1)),
        "no day after the day after until"
    );
}

#[tokio::test]
async fn each_meta_filter_must_match() {
    let day = days_ago(2);
    let tree = minted_on(day, 1);
    let days = Days::new();
    days.put(
        day,
        &[&[
            state(tree, day, 1, &[("region", "eu"), ("tier", "gold")]),
            state(tree, day, 2, &[("region", "eu")]),
            state(tree, day, 3, &[("tier", "gold")]),
            state(tree, day, 4, &[("region", "eu"), ("tier", "silver")]),
            state(
                tree,
                day,
                5,
                &[("region", "eu"), ("tier", "gold"), ("x", "1")],
            ),
        ]],
    )
    .await;
    let both = StatesFilter {
        meta: BTreeMap::from([
            ("region".into(), "eu".into()),
            ("tier".into(), "gold".into()),
        ]),
        ..filter(day, days_ago(1))
    };

    let found = states(&days, both).await;

    assert_eq!(offsets(&found, day), [1, 5]);
}

#[tokio::test]
async fn service_category_and_window_filter_the_states() {
    let day = days_ago(2);
    let tree = minted_on(day, 1);
    let other_service = StateRow {
        service: "shipping".into(),
        ..state(tree, day, 1, &[])
    };
    let other_category = StateRow {
        category: "other".into(),
        ..state(tree, day, 2, &[])
    };
    let split = state_row(tree, 0, at(day, 3), Some(1));
    let days = Days::new();
    days.put(
        day,
        &[&[
            other_service,
            other_category,
            split,
            state(tree, day, 4, &[]),
            state(tree, day, 3_600_000, &[]),
        ]],
    )
    .await;
    let window = StatesFilter {
        until: noon(day) + SignedDuration::from_secs(60),
        ..filter(day, day)
    };

    let found = states(&days, window).await;

    assert_eq!(offsets(&found, day), [4]);
}

#[tokio::test]
async fn the_walk_reads_from_the_day_after_until_back_to_the_day_of_since() {
    let days = Days::new();

    states(&days, filter(days_ago(5), days_ago(3))).await;

    assert_eq!(
        days.manifest_days(),
        [days_ago(2), days_ago(3), days_ago(4), days_ago(5)]
    );
}

#[tokio::test]
async fn days_before_find_a_state_stored_the_day_before_its_time() {
    let (day, flushed) = (days_ago(3), days_ago(4));
    let early = state(minted_on(day, 1), day, 0, &[]);
    let days = Days::new();
    days.put(flushed, &[std::slice::from_ref(&early)]).await;
    let with_days_before = |days_before| ScanConfig {
        days_before,
        ..ScanConfig::default()
    };

    let without = days
        .states_lookup(with_days_before(0))
        .states(filter(day, day), wanted(100), None)
        .await;
    let with = days
        .states_lookup(with_days_before(1))
        .states(filter(day, day), wanted(100), None)
        .await;

    assert_eq!(offsets(&without, day), [] as [i64; 0]);
    assert_eq!(offsets(&with, day), [0]);
}

#[tokio::test]
async fn the_walk_starts_tomorrow_at_the_latest() {
    let days = Days::new();
    let window = StatesFilter {
        until: Timestamp::now() + SignedDuration::from_hours(24 * 7),
        ..filter(days_ago(1), days_ago(1))
    };

    let tomorrow = around(|| days_ago(-1), async {
        states(&days, window).await;
    })
    .await;

    let walked = days.manifest_days();
    assert!(
        walked.first().is_some_and(|first| tomorrow.contains(first)),
        "{walked:?}"
    );
}

#[tokio::test]
async fn the_walk_ends_at_the_oldest_day_kept() {
    let days = Days::new();
    let oldest_kept =
        || ManifestCache::new(days.store.clone(), ManifestCacheConfig::default()).oldest_kept();
    let window = filter(days_ago(10_000), days_ago(1));

    let oldest = around(oldest_kept, async {
        states(&days, window).await;
    })
    .await;

    let walked = days.manifest_days();
    assert!(oldest.contains(&walked.last().copied()), "{walked:?}");
}

#[tokio::test]
async fn a_file_outside_the_window_is_not_opened() {
    let day = days_ago(2);
    let tree = minted_on(day, 1);
    let days = Days::new();
    days.put(
        day,
        &[
            &[state(tree, day, 0, &[])],
            &[state(tree, day, 2 * 3_600_000, &[])],
        ],
    )
    .await;
    let window = StatesFilter {
        until: noon(day) + SignedDuration::from_secs(60),
        ..filter(day, day)
    };

    let found = states(&days, window).await;

    assert_eq!(offsets(&found, day), [0]);
    let first = ObjectName::StatesFile {
        day,
        nnnn: UnixMillis::from(at(day, 0)),
    };
    assert_eq!(
        days.requests_on(day)
            .into_iter()
            .filter(|path| path.ends_with("-states.parquet"))
            .collect::<std::collections::BTreeSet<_>>(),
        [first.to_string()].into()
    );
}

#[tokio::test]
async fn the_walk_stops_when_it_has_the_states_wanted() {
    let (old, new) = (days_ago(3), days_ago(1));
    let days = Days::new();
    days.put(old, &[&[state(minted_on(old, 1), old, 0, &[])]])
        .await;
    days.put(
        new,
        &[
            &[state(minted_on(new, 1), new, 0, &[])],
            &[state(minted_on(new, 2), new, 1, &[])],
        ],
    )
    .await;

    let found = days
        .states_lookup(ScanConfig {
            concurrent_files: wanted(1),
            ..ScanConfig::default()
        })
        .states(filter(old, days_ago(0)), wanted(1), None)
        .await;

    assert_eq!(found.rows.len(), 1);
    assert_eq!(
        found.rows[0].tree,
        minted_on(new, 2),
        "the newest flush first"
    );
    let days_read = days
        .store
        .paths()
        .iter()
        .filter(|path| path.ends_with("-states.parquet"))
        .map(|path| path[..8].to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        days_read,
        [new.to_string()].into(),
        "no file of an older day"
    );
}

#[tokio::test]
async fn a_broken_manifest_is_reported_and_the_walk_goes_on() {
    let (old, broken) = (days_ago(3), days_ago(2));
    let days = Days::new();
    days.put(old, &[&[state(minted_on(old, 1), old, 0, &[])]])
        .await;
    days.put_object(
        &ObjectName::Manifest { day: broken },
        b"not a manifest".to_vec(),
    )
    .await;

    let found = states(&days, filter(old, broken)).await;

    assert_eq!(offsets(&found, old), [0]);
    let [problem] = found.problems.as_slice() else {
        panic!("one problem expected: {:?}", found.problems);
    };
    assert!(
        matches!(problem, StatesLookupError::Manifest(_)),
        "{problem:?}"
    );
}

#[tokio::test]
async fn a_states_file_that_is_gone_is_reported_and_the_walk_goes_on() {
    let (old, new) = (days_ago(3), days_ago(1));
    let days = Days::new();
    days.put(old, &[&[state(minted_on(old, 1), old, 0, &[])]])
        .await;
    days.put(new, &[&[state(minted_on(new, 1), new, 0, &[])]])
        .await;
    let gone = ObjectName::StatesFile {
        day: new,
        nnnn: UnixMillis::from(at(new, 0)),
    };
    days.store
        .inner
        .delete(&object_store::path::Path::from(gone.to_string()))
        .await
        .unwrap();

    let found = states(&days, filter(old, new)).await;

    assert_eq!(offsets(&found, old), [0]);
    let [StatesLookupError::File(problem)] = found.problems.as_slice() else {
        panic!("one file problem expected: {:?}", found.problems);
    };
    assert_eq!(problem.file(), &gone);
}

/// The lookup of `filter` in `order`, keeping `wanted` states.
async fn ordered(days: &Days, filter: StatesFilter, wanted: usize, order: TsOrder) -> FoundStates {
    days.states_lookup(ScanConfig {
        concurrent_files: NonZeroUsize::MIN,
        ..ScanConfig::default()
    })
    .states(filter, self::wanted(wanted), Some(order))
    .await
}

/// Two days of three flushes, whose times interleave: one state of the
/// first day is stored on the second day.
async fn interleaved() -> (Days, Day, Day) {
    let (first, second) = (days_ago(3), days_ago(2));
    let tree = minted_on(first, 1);
    let days = Days::new();
    days.put(
        first,
        &[
            &[state(tree, first, 30, &[]), state(tree, first, 10, &[])],
            &[state(tree, first, 20, &[]), state(tree, first, 50, &[])],
        ],
    )
    .await;
    days.put(
        second,
        &[&[state(tree, first, 25, &[]), state(tree, second, 0, &[])]],
    )
    .await;
    (days, first, second)
}

#[tokio::test]
async fn an_order_sorts_the_states_of_all_files_by_time() {
    let (days, first, second) = interleaved().await;
    let after_noon = (at(second, 0) - at(first, 0)) as i64;

    let ascending = ordered(&days, filter(first, second), 100, TsOrder::Ascending).await;
    let descending = ordered(&days, filter(first, second), 100, TsOrder::Descending).await;

    assert_eq!(offsets(&ascending, first), [10, 20, 25, 30, 50, after_noon]);
    assert_eq!(
        offsets(&descending, first),
        [after_noon, 50, 30, 25, 20, 10]
    );
    assert!(ascending.problems.is_empty(), "{:?}", ascending.problems);
}

#[tokio::test]
async fn an_order_keeps_only_the_first_states_wanted() {
    let (days, first, second) = interleaved().await;

    let ascending = ordered(&days, filter(first, second), 3, TsOrder::Ascending).await;
    let descending = ordered(&days, filter(first, second), 2, TsOrder::Descending).await;

    assert_eq!(offsets(&ascending, first), [10, 20, 25]);
    assert_eq!(
        offsets(&descending, second),
        [0, 50 - (at(second, 0) - at(first, 0)) as i64]
    );
}

#[tokio::test]
async fn an_ordered_lookup_does_not_open_a_file_that_cannot_improve_the_page() {
    let day = days_ago(2);
    let tree = minted_on(day, 1);
    let days = Days::new();
    days.put(
        day,
        &[
            &[
                state(tree, day, 0, &[]),
                state(tree, day, 1, &[]),
                state(tree, day, 2, &[]),
            ],
            &[state(tree, day, 100, &[])],
            &[state(tree, day, 200, &[])],
        ],
    )
    .await;

    let found = ordered(&days, filter(day, days_ago(1)), 2, TsOrder::Ascending).await;

    assert_eq!(offsets(&found, day), [0, 1]);
    let opened: Vec<String> = days
        .requests_on(day)
        .into_iter()
        .filter(|path| path.ends_with("-states.parquet"))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let first = ObjectName::StatesFile {
        day,
        nnnn: UnixMillis::from(at(day, 0)),
    };
    assert_eq!(opened, [first.to_string()]);
}

#[tokio::test]
async fn states_of_the_same_time_keep_one_order() {
    let day = days_ago(2);
    let days = Days::new();
    days.put(
        day,
        &[
            &[
                state(minted_on(day, 1), day, 5, &[]),
                state(minted_on(day, 2), day, 5, &[]),
            ],
            &[state(minted_on(day, 3), day, 5, &[])],
        ],
    )
    .await;
    let trees = |found: &FoundStates| found.rows.iter().map(|row| row.tree).collect::<Vec<_>>();

    let first = ordered(&days, filter(day, days_ago(1)), 100, TsOrder::Ascending).await;
    let again = ordered(&days, filter(day, days_ago(1)), 100, TsOrder::Ascending).await;

    assert_eq!(
        trees(&first),
        [minted_on(day, 1), minted_on(day, 2), minted_on(day, 3)],
        "the older flush first, then file order"
    );
    assert_eq!(trees(&first), trees(&again));
}

#[tokio::test]
async fn states_of_the_same_time_keep_one_order_when_files_are_read_together() {
    let day = days_ago(2);
    let days = Days::new();
    let flushes: Vec<Vec<StateRow>> = (0..4)
        .map(|flush| {
            (0..3)
                .map(|tree| state(minted_on(day, flush * 10 + tree), day, 5, &[]))
                .collect()
        })
        .collect();
    let flushes: Vec<&[StateRow]> = flushes.iter().map(Vec::as_slice).collect();
    days.put(day, &flushes).await;
    let lookup = days.states_lookup(ScanConfig::default());
    let trees = |found: FoundStates| {
        found
            .rows
            .into_iter()
            .map(|row| row.tree)
            .collect::<Vec<_>>()
    };

    let mut answers = Vec::new();
    for _ in 0..5 {
        let found = lookup
            .states(
                filter(day, days_ago(1)),
                wanted(5),
                Some(TsOrder::Ascending),
            )
            .await;
        answers.push(trees(found));
    }

    let expected: Vec<TreeId> = [0, 1, 2, 10, 11]
        .map(|random| minted_on(day, random))
        .into();
    for answer in answers {
        assert_eq!(answer, expected, "the oldest flush first, then file order");
    }
}
