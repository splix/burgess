//! Retention moves between pages: a cursor expires when it deletes rows
//! that the rest of the answer reads, and only then.
//!
//! A reader that keeps fewer days is the reader of a later day.

use std::num::NonZeroU32;

use burgess_api::Page;
use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{
    GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest, Order,
};
use burgess_core::flow::{FlowId, Subflow};
use burgess_core::storage::Day;

use super::{RequestError, reference};
use crate::manifests::ManifestCacheConfig;
use crate::test_support::{Days, at, days_ago, flush, minted_on, noon, state_row};

/// A reader that keeps the last `days` days.
fn keeping(days: u32) -> ManifestCacheConfig {
    ManifestCacheConfig {
        retention_days: NonZeroU32::new(days).unwrap(),
        ..ManifestCacheConfig::default()
    }
}

fn refs_request(since: Option<Day>, cursor: Option<String>) -> ListRefsRequest {
    ListRefsRequest {
        ref_type: "order_id".into(),
        domain_id: "A".into(),
        since_ms: since.map(|day| noon(day).as_millisecond()),
        limit: Some(1),
        cursor,
        ..Default::default()
    }
}

/// The references walk newest day first, thus the rest of the answer is
/// on the oldest day.
#[tokio::test]
async fn a_references_cursor_expires_when_retention_deletes_the_rest() {
    let (old, recent) = (days_ago(6), days_ago(2));
    let tree = minted_on(old, 1);
    let days = Days::new();
    days.put_refs(old, &[&[reference("A", tree, 0, at(old, 0))]])
        .await;
    days.put_refs(recent, &[&[reference("A", tree, 1, at(recent, 0))]])
        .await;
    let first = Page::from_responses(
        days.queries()
            .list_refs(refs_request(None, None))
            .await
            .unwrap(),
    );

    let rest = days
        .queries_with(keeping(4))
        .list_refs(refs_request(None, first.cursor))
        .await;

    assert!(matches!(rest, Err(RequestError::Expired)), "{rest:?}");
}

/// With `since`, the walk stops at a day that retention still keeps.
#[tokio::test]
async fn a_references_cursor_whose_walk_stops_inside_retention_continues() {
    let day = days_ago(2);
    let tree = minted_on(day, 1);
    let days = Days::new();
    days.put_refs(
        day,
        &[
            &[reference("A", tree, 0, at(day, 0))],
            &[reference("A", tree, 1, at(day, 1))],
        ],
    )
    .await;
    let since = Some(days_ago(3));
    let first = Page::from_responses(
        days.queries()
            .list_refs(refs_request(since, None))
            .await
            .unwrap(),
    );

    let rest = Page::from_responses(
        days.queries_with(keeping(10))
            .list_refs(refs_request(since, first.cursor))
            .await
            .unwrap(),
    );

    let subflows: Vec<u32> = rest.rows.iter().map(|row| row.subflow.into()).collect();
    assert_eq!(subflows, [0]);
    assert_eq!(rest.cursor, None);
}

/// The flows below a flow are known from the splits of the whole tree, thus
/// the rest of a flow needs the day that it split on, also when the cursor
/// is on a later day.
#[tokio::test]
async fn a_flow_cursor_expires_when_retention_deletes_a_split_of_the_flow() {
    let (split, later) = (days_ago(6), days_ago(3));
    let tree = minted_on(split, 1);
    let days = Days::new();
    days.put(
        split,
        &[&flush(vec![vec![
            state_row(tree, 0, at(split, 0), Some(1)),
            state_row(tree, 1, at(split, 1), Some(2)),
        ]])],
    )
    .await;
    days.put(
        later,
        &[&flush(vec![vec![
            state_row(tree, 2, at(later, 0), None),
            state_row(tree, 2, at(later, 1), None),
        ]])],
    )
    .await;
    let request = |cursor: Option<String>| GetFlowRequest {
        flow_id: FlowId::new(tree, Subflow::from(1)).to_string(),
        limit: Some(2),
        cursor,
        ..Default::default()
    };
    let first = Page::from_responses(days.queries().get_flow(request(None)).await.unwrap());

    let rest = days
        .queries_with(keeping(4))
        .get_flow(request(first.cursor))
        .await;

    assert!(matches!(rest, Err(RequestError::Expired)), "{rest:?}");
}

/// A tree walks oldest day first, thus the rest of the answer is on the day
/// of the cursor and after it.
#[tokio::test]
async fn a_tree_cursor_continues_when_retention_deletes_only_the_pages_before() {
    let (minted, later) = (days_ago(6), days_ago(3));
    let tree = minted_on(minted, 1);
    let days = Days::new();
    days.put(
        minted,
        &[&flush(vec![vec![state_row(tree, 0, at(minted, 0), None)]])],
    )
    .await;
    let rest_of_tree = state_row(tree, 0, at(later, 1), None);
    days.put(
        later,
        &[&flush(vec![vec![
            state_row(tree, 0, at(later, 0), None),
            rest_of_tree.clone(),
        ]])],
    )
    .await;
    let request = |cursor: Option<String>| GetTreeRequest {
        tree: Some(Tree::TreeId(tree.to_string())),
        limit: Some(2),
        cursor,
        ..Default::default()
    };
    let first = Page::from_responses(days.queries().get_tree(request(None)).await.unwrap());

    let rest = Page::from_responses(
        days.queries_with(keeping(4))
            .get_tree(request(first.cursor))
            .await
            .unwrap(),
    );

    let times: Vec<_> = rest.rows.iter().map(|row| row.ts).collect();
    assert_eq!(times, [rest_of_tree.time.saturating_timestamp()]);
    assert_eq!(rest.cursor, None);
}

/// In descending time order the rest of the answer is on the oldest days.
#[tokio::test]
async fn a_states_cursor_by_time_expires_when_retention_deletes_the_rest() {
    let (old, recent) = (days_ago(6), days_ago(2));
    let tree = minted_on(old, 1);
    let days = Days::new();
    for day in [old, recent] {
        days.put(
            day,
            &[&flush(vec![vec![state_row(tree, 0, at(day, 0), None)]])],
        )
        .await;
    }
    let request = |cursor: Option<String>| ListStatesRequest {
        service: "checkout".into(),
        category: "step".into(),
        since_ms: Some(noon(days_ago(7)).as_millisecond()),
        order: Order::TsDescending.into(),
        limit: Some(1),
        cursor,
        ..Default::default()
    };
    let first = Page::from_responses(days.queries().list_states(request(None)).await.unwrap());

    let rest = days
        .queries_with(keeping(4))
        .list_states(request(first.cursor))
        .await;

    assert!(matches!(rest, Err(RequestError::Expired)), "{rest:?}");
}
