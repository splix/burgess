//! Refs lookups over days of real files: which rows, which days and which
//! files.

use std::num::NonZeroUsize;

use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::{Day, ObjectName, RefRow};
use burgess_core::wire::UnixMillis;
use jiff::Timestamp;
use object_store::ObjectStoreExt;

use super::*;
use crate::refs_file::RefKey;
use crate::test_support::{Days, around, at, days_ago, minted_on, noon};

/// A reference to `domain_id` from subflow `subflow` of `tree`, `offset`
/// milliseconds after noon of `day`.
fn reference(domain_id: &str, tree: TreeId, subflow: u32, day: Day, offset: u64) -> RefRow {
    RefRow {
        ref_type: "order_id".into(),
        domain_id: domain_id.into(),
        service: "checkout".into(),
        category: Some("payment".into()),
        tree,
        subflow: Subflow::from(subflow),
        time: UnixMillis::from(at(day, offset)),
    }
}

fn query(domain_id: &str) -> RefsQuery {
    RefsQuery {
        key: RefKey::new("order_id".into(), domain_id.into()).unwrap(),
        service: None,
        category: None,
        since: None,
        until: None,
    }
}

fn wanted(rows: usize) -> NonZeroUsize {
    NonZeroUsize::new(rows).unwrap()
}

async fn refs(days: &Days, query: &RefsQuery) -> FoundRefs {
    days.refs_lookup(RefsLookupConfig::default())
        .refs(query, wanted(100))
        .await
        .unwrap()
}

/// The flow of each row, as `(tree, subflow)`.
fn flows(found: &FoundRefs) -> Vec<(TreeId, u32)> {
    found
        .rows
        .iter()
        .map(|row| (row.tree, u32::from(row.subflow)))
        .collect()
}

#[tokio::test]
async fn one_id_used_by_several_flows_gives_a_flat_row_for_each_newest_day_first() {
    let (old, new) = (days_ago(3), days_ago(1));
    let (first, second) = (minted_on(old, 1), minted_on(new, 2));
    let days = Days::new();
    days.put_refs(
        old,
        &[&[
            reference("ORD-1", first, 0, old, 0),
            reference("ORD-1", first, 3, old, 1),
            reference("ORD-2", first, 4, old, 2),
        ]],
    )
    .await;
    days.put_refs(new, &[&[reference("ORD-1", second, 1, new, 0)]])
        .await;

    let found = refs(&days, &query("ORD-1")).await;

    assert_eq!(flows(&found), [(second, 1), (first, 0), (first, 3)]);
    assert!(found.problems.is_empty(), "{:?}", found.problems);
}

#[tokio::test]
async fn the_lookup_is_case_sensitive() {
    let day = days_ago(1);
    let days = Days::new();
    days.put_refs(day, &[&[reference("ORD-1", minted_on(day, 1), 0, day, 0)]])
        .await;

    assert!(refs(&days, &query("ord-1")).await.rows.is_empty());
}

#[tokio::test]
async fn service_category_since_and_until_filter_the_rows() {
    let day = days_ago(2);
    let tree = minted_on(day, 1);
    let mut other_service = reference("ORD-1", tree, 1, day, 10);
    other_service.service = "billing".into();
    let mut no_category = reference("ORD-1", tree, 2, day, 20);
    no_category.category = None;
    let days = Days::new();
    days.put_refs(
        day,
        &[&[
            reference("ORD-1", tree, 0, day, 0),
            other_service,
            no_category,
            reference("ORD-1", tree, 3, day, 30),
        ]],
    )
    .await;
    let time = |offset: u64| Timestamp::from_millisecond(at(day, offset) as i64).unwrap();
    let subflows = |found: FoundRefs| -> Vec<u32> {
        let mut subflows: Vec<u32> = found.rows.iter().map(|row| row.subflow.into()).collect();
        subflows.sort();
        subflows
    };

    let cases = [
        (
            RefsQuery {
                service: Some("billing".into()),
                ..query("ORD-1")
            },
            vec![1],
        ),
        (
            RefsQuery {
                category: Some("payment".into()),
                ..query("ORD-1")
            },
            vec![0, 1, 3],
        ),
        (
            RefsQuery {
                since: Some(time(10)),
                ..query("ORD-1")
            },
            vec![1, 2, 3],
        ),
        (
            RefsQuery {
                until: Some(time(20)),
                ..query("ORD-1")
            },
            vec![0, 1, 2],
        ),
        (
            RefsQuery {
                since: Some(time(10)),
                until: Some(time(20)),
                ..query("ORD-1")
            },
            vec![1, 2],
        ),
    ];
    for (query, expected) in cases {
        assert_eq!(subflows(refs(&days, &query).await), expected, "{query:?}");
    }
}

#[tokio::test]
async fn a_reference_stored_the_day_after_its_time_is_found() {
    let (day, flushed) = (days_ago(3), days_ago(2));
    let late = reference("ORD-1", minted_on(day, 1), 0, day, 11 * 3_600_000);
    let days = Days::new();
    days.put_refs(flushed, &[std::slice::from_ref(&late)]).await;
    let until = noon(day) + jiff::SignedDuration::from_mins(11 * 60 + 30);

    let found = refs(
        &days,
        &RefsQuery {
            until: Some(until),
            ..query("ORD-1")
        },
    )
    .await;

    assert_eq!(found.rows, [late]);
    assert!(
        !days.manifest_days().contains(&days_ago(1)),
        "no day after the day after until"
    );
}

#[tokio::test]
async fn a_day_without_references_costs_its_manifest_only() {
    let day = days_ago(1);
    let days = Days::new();
    days.put(
        day,
        &[&[crate::test_support::state_row(
            minted_on(day, 1),
            0,
            at(day, 0),
            None,
        )]],
    )
    .await;

    let found = refs(&days, &query("ORD-1")).await;

    assert!(found.rows.is_empty());
    assert_eq!(days.requests_on(day), [format!("{day}/_manifest")]);
}

#[tokio::test]
async fn a_broken_manifest_is_reported_and_the_walk_goes_on() {
    let (old, broken) = (days_ago(3), days_ago(2));
    let tree = minted_on(old, 1);
    let days = Days::new();
    days.put_refs(old, &[&[reference("ORD-1", tree, 0, old, 0)]])
        .await;
    days.put_object(
        &ObjectName::Manifest { day: broken },
        b"not a manifest".to_vec(),
    )
    .await;

    let found = refs(&days, &query("ORD-1")).await;

    assert_eq!(flows(&found), [(tree, 0)]);
    let [problem] = found.problems.as_slice() else {
        panic!("one problem expected: {:?}", found.problems);
    };
    assert!(
        matches!(problem, RefsLookupError::Manifest(_)),
        "{problem:?}"
    );
}

#[tokio::test]
async fn the_walk_stops_when_it_has_the_rows_wanted() {
    let (old, new) = (days_ago(3), days_ago(1));
    let days = Days::new();
    days.put_refs(old, &[&[reference("ORD-1", minted_on(old, 1), 0, old, 0)]])
        .await;
    days.put_refs(
        new,
        &[
            &[reference("ORD-1", minted_on(new, 1), 0, new, 0)],
            &[reference("ORD-1", minted_on(new, 2), 0, new, 1)],
        ],
    )
    .await;

    let found = days
        .refs_lookup(RefsLookupConfig {
            concurrent_files: wanted(1),
            ..RefsLookupConfig::default()
        })
        .refs(&query("ORD-1"), wanted(1))
        .await
        .unwrap();

    assert_eq!(found.rows.len(), 1);
    assert_eq!(
        found.rows[0].tree,
        minted_on(new, 2),
        "the newest flush first"
    );
    let refs_files_read = days
        .store
        .paths()
        .iter()
        .filter(|path| path.ends_with("-refs.parquet"))
        .map(|path| path[..8].to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        refs_files_read.into_iter().collect::<Vec<_>>(),
        [new.to_string()],
        "no file of an older day is read"
    );
}

#[tokio::test]
async fn since_ends_the_walk_on_its_day() {
    let since_day = days_ago(3);
    let days = Days::new();
    let since = RefsQuery {
        since: Some(noon(since_day)),
        ..query("ORD-1")
    };

    let tomorrow = around(|| days_ago(-1), async {
        refs(&days, &since).await;
    })
    .await;

    let walked = days.manifest_days();
    assert_eq!(walked.last(), Some(&since_day), "{walked:?}");
    assert!(
        walked.first().is_some_and(|first| tomorrow.contains(first)),
        "the walk starts tomorrow: {walked:?}"
    );
}

#[tokio::test]
async fn days_before_extend_the_walk_before_since() {
    let since_day = days_ago(3);
    let days = Days::new();
    let config = RefsLookupConfig {
        days_before: 1,
        ..RefsLookupConfig::default()
    };

    days.refs_lookup(config)
        .refs(
            &RefsQuery {
                since: Some(noon(since_day)),
                ..query("ORD-1")
            },
            wanted(100),
        )
        .await
        .unwrap();

    assert_eq!(days.manifest_days().last(), Some(&days_ago(4)));
}

#[tokio::test]
async fn without_since_the_walk_ends_at_the_oldest_day_kept() {
    let days = Days::new();
    let oldest_kept = || {
        crate::manifests::ManifestCache::new(
            days.store.clone(),
            crate::manifests::ManifestCacheConfig::default(),
        )
        .oldest_kept()
    };

    let oldest = around(oldest_kept, async {
        refs(&days, &query("ORD-1")).await;
    })
    .await;

    let walked = days.manifest_days();
    assert!(oldest.contains(&walked.last().copied()), "{walked:?}");
}

#[tokio::test]
async fn a_walk_without_an_end_is_refused() {
    let days = Days::new();
    let manifests = crate::manifests::ManifestCache::new(
        days.store.clone(),
        crate::manifests::ManifestCacheConfig {
            retention_days: std::num::NonZeroU32::MAX,
            ..crate::manifests::ManifestCacheConfig::default()
        },
    );
    let files = crate::refs_file::RefsFileReader::new(
        days.store.clone(),
        crate::parquet_file::FileReadConfig::default(),
    );
    let lookup = RefsLookup::new(
        std::sync::Arc::new(manifests),
        files,
        RefsLookupConfig::default(),
    );

    let unbounded = lookup.refs(&query("ORD-1"), wanted(1)).await;
    let bounded = lookup
        .refs(
            &RefsQuery {
                since: Some(noon(days_ago(2))),
                ..query("ORD-1")
            },
            wanted(1),
        )
        .await;

    assert_eq!(unbounded.err(), Some(UnboundedWalk));
    assert!(bounded.is_ok());
    assert!(
        days.manifest_days().len() <= 4,
        "{:?}",
        days.manifest_days()
    );
}

#[tokio::test]
async fn a_refs_file_that_is_gone_is_reported_and_the_walk_goes_on() {
    let (old, new) = (days_ago(3), days_ago(1));
    let tree = minted_on(old, 1);
    let days = Days::new();
    days.put_refs(old, &[&[reference("ORD-1", tree, 0, old, 0)]])
        .await;
    days.put_refs(new, &[&[reference("ORD-1", tree, 1, new, 0)]])
        .await;
    let gone = ObjectName::RefsFile {
        day: new,
        nnnn: UnixMillis::from(at(new, 0)),
    };
    days.store
        .inner
        .delete(&object_store::path::Path::from(gone.to_string()))
        .await
        .unwrap();

    let found = refs(&days, &query("ORD-1")).await;

    assert_eq!(flows(&found), [(tree, 0)]);
    let [RefsLookupError::File(problem)] = found.problems.as_slice() else {
        panic!("one file problem expected: {:?}", found.problems);
    };
    assert_eq!(problem.file(), &gone);
}
