//! The bloom filters save requests and change no answer.
//!
//! The days are stored by the writer, with its filters, thus a key that the
//! reader makes otherwise than the writer is not found. The same answers
//! without the filters are the answers of a day the writer stored without
//! them.

use std::time::Duration;

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{GetTreeRequest, ListRefsRequest};
use burgess_core::flow::TreeId;
use burgess_core::storage::{Day, ObjectName, RefRow};
use burgess_core::wire::UnixMillis;
use jiff::{SignedDuration, Timestamp};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use serde_json::Value;

use super::{pages, reference, whole};
use crate::manifests::ManifestCacheConfig;
use crate::test_support::{Days, at, days_ago, minted_on, state_row};

fn refs_request(domain_id: &str) -> ListRefsRequest {
    ListRefsRequest {
        ref_type: "order_id".into(),
        domain_id: domain_id.into(),
        ..Default::default()
    }
}

fn tree_request(tree: TreeId) -> GetTreeRequest {
    GetTreeRequest {
        tree: Some(Tree::TreeId(tree.to_string())),
        ..Default::default()
    }
}

/// `offset` milliseconds past noon of `day`, as a time.
fn time(day: Day, offset: u64) -> Timestamp {
    Timestamp::from_millisecond(at(day, offset) as i64).unwrap()
}

/// References of two days, one flush each, and a tree on both.
async fn two_days() -> (Days, [Day; 2], TreeId) {
    let days = Days::new();
    let (older, newer) = (days_ago(3), days_ago(2));
    let tree = minted_on(older, 5);
    for (day, ids) in [(older, ["ORD-1", "ORD-2"]), (newer, ["ORD-3", "ORD-1"])] {
        let refs: Vec<RefRow> = ids
            .iter()
            .enumerate()
            .map(|(subflow, id)| reference(id, tree, subflow as u32, at(day, 0)))
            .collect();
        let states = vec![state_row(tree, 0, at(day, 0), None)];
        days.flush_at(time(day, 0), states, refs).await;
    }
    (days, [older, newer], tree)
}

/// The JSON answers to a set of lookups.
async fn answers(days: &Days, tree: TreeId) -> Vec<Vec<Value>> {
    let queries = days.queries();
    let mut answers = Vec::new();
    for id in ["ORD-1", "ORD-2", "ORD-3", " ORD-1 ", "ord-1", "ORD-404"] {
        answers.push(whole(queries.list_refs(refs_request(id)).await).await);
    }
    answers.push(whole(queries.get_tree(tree_request(tree)).await).await);
    answers
}

#[tokio::test]
async fn the_filters_change_no_answer() {
    let (days, stored_days, tree) = two_days().await;

    let with = answers(&days, tree).await;
    let queries = days.queries();
    let paged = pages(async |cursor| {
        let request = ListRefsRequest {
            limit: Some(1),
            cursor,
            ..refs_request("ORD-1")
        };
        queries.list_refs(request).await
    })
    .await;
    for day in stored_days {
        days.delete_blooms(day).await;
    }
    let without = answers(&days, tree).await;

    assert_eq!(with, without);
    assert_eq!(paged.concat(), with[0], "pages of one reference");
    let found = |answer: &Vec<Value>| answer.len();
    assert_eq!(
        with.iter().map(found).collect::<Vec<_>>(),
        [2, 1, 1, 2, 0, 0, 2]
    );
}

/// The guiding rule of the lookups: a negative day costs one request.
#[tokio::test]
async fn a_day_that_the_filter_rules_out_costs_one_request() {
    let (days, [older, newer], _) = two_days().await;

    let found = whole(days.queries().list_refs(refs_request("ORD-3")).await).await;

    assert_eq!(found.len(), 1);
    assert_eq!(days.requests_on(older), [format!("{older}/refs.bloom")]);
    assert!(
        days.requests_on(newer)
            .contains(&format!("{newer}/manifest")),
        "{:?}",
        days.requests_on(newer)
    );
}

/// The file-level key opens only the refs file that can hold the
/// reference.
#[tokio::test]
async fn a_file_that_the_filter_rules_out_is_not_opened() {
    let days = Days::new();
    let day = days_ago(2);
    let tree = minted_on(day, 5);
    let first = days
        .flush_at(
            time(day, 0),
            Vec::new(),
            vec![reference("ORD-1", tree, 0, at(day, 0))],
        )
        .await;
    let second = days
        .flush_at(
            time(day, 1),
            Vec::new(),
            vec![reference("ORD-2", tree, 0, at(day, 1))],
        )
        .await;

    let found = whole(days.queries().list_refs(refs_request("ORD-2")).await).await;

    assert_eq!(found.len(), 1);
    let opened = |nnnn: UnixMillis| {
        let file = ObjectName::RefsFile { day, nnnn }.to_string();
        days.requests_on(day).contains(&file)
    };
    assert!(!opened(first), "{:?}", days.requests_on(day));
    assert!(opened(second), "{:?}", days.requests_on(day));
}

/// A filter answers only for the files listed when it was read. A lookup
/// that used a filter read before would not find what was flushed since.
#[tokio::test]
async fn a_reference_flushed_after_a_lookup_is_found_by_the_next() {
    let days = Days::new();
    let day = days_ago(0);
    let tree = minted_on(day, 5);
    let start = Timestamp::now() - SignedDuration::from_mins(2);
    days.flush_at(
        start,
        Vec::new(),
        vec![reference("ORD-1", tree, 0, at(day, 0))],
    )
    .await;
    let queries = days.queries_with(ManifestCacheConfig {
        unsealed_ttl: Duration::ZERO,
        ..ManifestCacheConfig::default()
    });
    let before = whole(queries.list_refs(refs_request("ORD-2")).await).await;

    days.flush_at(
        start + SignedDuration::from_mins(1),
        Vec::new(),
        vec![reference("ORD-2", tree, 1, at(day, 1))],
    )
    .await;
    let after = whole(queries.list_refs(refs_request("ORD-2")).await).await;

    assert!(before.is_empty());
    assert_eq!(after.len(), 1);
}

/// A filter of another day does not hold the keys of this one.
#[tokio::test]
async fn a_filter_of_another_day_is_not_used() {
    let (days, [older, newer], _) = two_days().await;
    let copied = days
        .store
        .inner
        .get(&Path::from(
            ObjectName::RefsBloom { day: older }.to_string(),
        ))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let newer_filter = Path::from(ObjectName::RefsBloom { day: newer }.to_string());
    days.store
        .inner
        .put(&newer_filter, copied.into())
        .await
        .unwrap();

    let found = whole(days.queries().list_refs(refs_request("ORD-3")).await).await;

    assert_eq!(found.len(), 1);
}

/// `count` flushes of `day` whose states files hold trees minted with
/// `tree` on `minted`, around it but not it, thus their bounds admit it.
async fn around_the_tree(days: &Days, day: Day, minted: Day, tree: TreeId, count: u64) {
    let (below, above) = (minted_on(minted, 1), minted_on(minted, 9));
    assert!(below < tree && tree < above);
    for flush in 0..count {
        let states = vec![
            state_row(below, 0, at(day, flush), None),
            state_row(above, 0, at(day, flush), None),
        ];
        days.flush_at(time(day, 100 + flush), states, Vec::new())
            .await;
    }
}

#[tokio::test]
async fn a_tree_lookup_reads_the_filter_only_above_the_candidate_threshold() {
    let days = Days::new();
    let (few, many) = (days_ago(3), days_ago(2));
    let tree = minted_on(few, 5);
    around_the_tree(&days, few, few, tree, 4).await;
    around_the_tree(&days, many, few, tree, 5).await;

    let found = whole(days.queries().get_tree(tree_request(tree)).await).await;

    assert!(found.is_empty());
    let parquet = |day: Day| {
        days.requests_on(day)
            .iter()
            .filter(|path| path.ends_with(".parquet"))
            .count()
    };
    assert!(
        !days
            .requests_on(few)
            .contains(&format!("{few}/trees.bloom"))
    );
    assert!(parquet(few) > 0);
    assert!(
        days.requests_on(many)
            .contains(&format!("{many}/trees.bloom"))
    );
    assert_eq!(parquet(many), 0, "{:?}", days.requests_on(many));
}

/// A day whose candidates the filter rules out still counts as a day with
/// candidates for when the walk stops, thus the walk goes as far as
/// without the filter: here past two such days, with a slack of one.
#[tokio::test]
async fn the_filter_does_not_end_a_tree_lookup_sooner() {
    let days = Days::new();
    let [minted, quiet, quieter, last] = [days_ago(4), days_ago(3), days_ago(2), days_ago(1)];
    let tree = minted_on(minted, 5);
    days.flush_at(
        time(minted, 0),
        vec![state_row(tree, 0, at(minted, 0), None)],
        Vec::new(),
    )
    .await;
    for day in [quiet, quieter] {
        around_the_tree(&days, day, minted, tree, 5).await;
    }
    days.flush_at(
        time(last, 0),
        vec![state_row(tree, 0, at(last, 0), None)],
        Vec::new(),
    )
    .await;

    let found = whole(days.queries().get_tree(tree_request(tree)).await).await;

    assert_eq!(found.len(), 2);
    assert!(
        days.requests_on(quiet)
            .contains(&format!("{quiet}/trees.bloom"))
    );
}

/// A filter that cannot be read, or a store that fails to give it, tells
/// nothing of the day: the day is read whole.
#[tokio::test]
async fn a_filter_that_cannot_be_read_rules_nothing_out() {
    let (days, [older, newer], _) = two_days().await;
    for day in [older, newer] {
        let filter = Path::from(ObjectName::RefsBloom { day }.to_string());
        days.store
            .inner
            .put(&filter, b"BGSB damaged".to_vec().into())
            .await
            .unwrap();
    }
    let damaged = whole(days.queries().list_refs(refs_request("ORD-1")).await).await;

    let (days, ..) = two_days().await;
    days.store.fail_reads_of(Some("refs.bloom"));
    let unreadable = whole(days.queries().list_refs(refs_request("ORD-1")).await).await;

    assert_eq!(damaged.len(), 2);
    assert_eq!(unreadable.len(), 2);
}

/// Above the threshold the filter rules out the files without the tree and
/// lets through the one with it, whose rows the lookup then reads.
#[tokio::test]
async fn a_tree_lookup_reads_the_files_that_the_filter_lets_through() {
    let days = Days::new();
    let day = days_ago(2);
    let tree = minted_on(day, 5);
    around_the_tree(&days, day, day, tree, 4).await;
    let holding = days
        .flush_at(
            time(day, 200),
            vec![state_row(tree, 0, at(day, 7), None)],
            Vec::new(),
        )
        .await;

    let found = whole(days.queries().get_tree(tree_request(tree)).await).await;

    assert_eq!(found.len(), 1);
    let requests = days.requests_on(day);
    assert!(
        requests.contains(&format!("{day}/trees.bloom")),
        "{requests:?}"
    );
    let holding_file = ObjectName::StatesFile { day, nnnn: holding }.to_string();
    let opened: Vec<&String> = requests
        .iter()
        .filter(|path| path.ends_with(".parquet"))
        .collect();
    assert!(
        !opened.is_empty() && opened.iter().all(|path| **path == holding_file),
        "{opened:?}"
    );
}
