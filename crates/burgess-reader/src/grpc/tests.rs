//! Calls to the gRPC service on days of real files: the messages of the
//! answers, and the requests that the service refuses.

use std::sync::Arc;

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::query_service_server::QueryService;
use burgess_api::grpc::{
    GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest, Order, get_flow_response,
    get_tree_response::Item,
};
use burgess_api::{Row, RowKind};
use burgess_core::flow::{FlowId, Subflow, TreeId};
use burgess_core::storage::{Day, ObjectName, StateRow};
use futures::StreamExt;
use tonic::{Code, Request, Status};

use super::*;
use crate::pages::PageLimits;
use crate::test_support::{Days, at, days_ago, flush, minted_on, noon, state_row};
use crate::tree_lookup::TreeLookupConfig;

/// A tree of four flows written on `day`: the root splits off 1 and 4, and
/// 1 splits off 2.
fn family(tree: TreeId, day: Day) -> Vec<StateRow> {
    flush(vec![vec![
        state_row(tree, 0, at(day, 0), None),
        state_row(tree, 0, at(day, 1), Some(1)),
        state_row(tree, 0, at(day, 2), Some(4)),
        state_row(tree, 1, at(day, 3), Some(2)),
        state_row(tree, 2, at(day, 4), None),
        state_row(tree, 4, at(day, 5), None),
    ]])
}

/// A store with one family tree written three days ago.
async fn one_tree() -> (Days, TreeId) {
    let day = days_ago(3);
    let tree = minted_on(day, 5);
    let days = Days::new();
    days.put(day, &[&family(tree, day)]).await;
    (days, tree)
}

fn api(days: &Days) -> QueryApi {
    QueryApi::new(Arc::new(Queries {
        lookup: days.lookup(TreeLookupConfig::default()),
        limits: PageLimits::default(),
    }))
}

fn tree_request(tree: TreeId) -> GetTreeRequest {
    GetTreeRequest {
        tree: Some(Tree::TreeId(tree.to_string())),
        ..Default::default()
    }
}

fn flow_request(tree: TreeId, subflow: u32) -> GetFlowRequest {
    GetFlowRequest {
        flow_id: FlowId::new(tree, Subflow::from(subflow)).to_string(),
        ..Default::default()
    }
}

/// The items of the messages of an answer.
async fn items<R>(answer: Result<tonic::Response<Answer<R>>, Status>) -> Vec<R> {
    answer
        .expect("an answer")
        .into_inner()
        .map(|message| message.expect("a message"))
        .collect()
        .await
}

async fn tree(days: &Days, request: GetTreeRequest) -> Vec<Item> {
    let answer = api(days).get_tree(Request::new(request)).await;
    items(answer)
        .await
        .into_iter()
        .map(|message| message.item.expect("an item"))
        .collect()
}

async fn tree_items(days: &Days, tree: TreeId) -> Vec<Item> {
    self::tree(days, tree_request(tree)).await
}

/// The GetFlow answer as GetTree items, which have the same content, thus
/// the same helpers check the two.
async fn flow(days: &Days, request: GetFlowRequest) -> Vec<Item> {
    let answer = api(days).get_flow(Request::new(request)).await;
    items(answer)
        .await
        .into_iter()
        .map(|message| match message.item.expect("an item") {
            get_flow_response::Item::Row(row) => Item::Row(row),
            get_flow_response::Item::Problem(problem) => Item::Problem(problem),
            get_flow_response::Item::Truncated(truncated) => Item::Truncated(truncated),
        })
        .collect()
}

fn rows(items: &[Item]) -> Vec<Row> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::Row(row) => Some(Row::try_from(row.clone()).expect("a readable row")),
            _ => None,
        })
        .collect()
}

fn subflows(items: &[Item]) -> Vec<u32> {
    rows(items)
        .iter()
        .map(|row| u32::from(row.subflow))
        .collect()
}

fn refused<T: std::fmt::Debug>(answer: Result<T, Status>) -> String {
    let status = answer.expect_err("a refusal");
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    status.message().to_string()
}

#[tokio::test]
async fn a_tree_answers_with_a_message_per_row() {
    let (days, tree) = one_tree().await;

    let items = tree_items(&days, tree).await;

    assert_eq!(subflows(&items), [0, 0, 0, 1, 2, 4]);
    assert_eq!(items.len(), 6, "rows only: {items:?}");
    let rows = rows(&items);
    assert_eq!((rows[0].kind, rows[0].tree), (RowKind::State, tree));
    assert_eq!(rows[0].payload.as_ref().unwrap().get(), "{}");
    assert_eq!(rows[1].kind, RowKind::Split);
    assert_eq!(rows[1].child_subflow, Some(Subflow::from(1)));
}

#[tokio::test]
async fn a_flow_id_gives_the_whole_tree_of_the_flow() {
    let (days, tree) = one_tree().await;
    let flow = FlowId::new(tree, Subflow::from(2));
    let request = GetTreeRequest {
        tree: Some(Tree::FlowId(flow.to_string())),
        ..Default::default()
    };

    let items = self::tree(&days, request).await;

    assert_eq!(subflows(&items), [0, 0, 0, 1, 2, 4]);
}

#[tokio::test]
async fn a_tree_needs_an_id_of_the_kind_its_field_names() {
    let (days, tree) = one_tree().await;
    let flow = FlowId::new(tree, Subflow::from(2));
    let mixed_up = [
        Tree::TreeId(flow.to_string()),
        Tree::FlowId(tree.to_string()),
    ];
    for id in mixed_up {
        let request = GetTreeRequest {
            tree: Some(id.clone()),
            ..Default::default()
        };
        let error = refused(api(&days).get_tree(Request::new(request)).await);
        assert!(error.starts_with("invalid id: expected"), "{id:?}: {error}");
    }
}

#[tokio::test]
async fn a_tree_without_an_id_is_refused() {
    let days = Days::new();

    let error = refused(
        api(&days)
            .get_tree(Request::new(GetTreeRequest::default()))
            .await,
    );

    assert_eq!(error, "tree_id or flow_id is necessary");
}

#[tokio::test]
async fn a_flow_answers_with_itself_and_the_flows_below_it() {
    let (days, tree) = one_tree().await;

    let items = flow(&days, flow_request(tree, 1)).await;

    assert_eq!(subflows(&items), [1, 2]);
}

#[tokio::test]
async fn a_flow_needs_a_flow_id_not_a_tree_id() {
    let (days, tree) = one_tree().await;
    let request = GetFlowRequest {
        flow_id: tree.to_string(),
        ..Default::default()
    };

    let error = refused(api(&days).get_flow(Request::new(request)).await);

    assert!(error.starts_with("invalid id: expected"), "{error}");
}

#[tokio::test]
async fn ids_that_are_not_hex_or_of_no_known_length_are_refused_before_any_read() {
    let days = Days::new();
    for id in ["zz", "0123abcd"] {
        let request = GetTreeRequest {
            tree: Some(Tree::TreeId(id.into())),
            ..Default::default()
        };
        let error = refused(api(&days).get_tree(Request::new(request)).await);
        assert!(error.starts_with("invalid id"), "{id}: {error}");
    }
    assert!(days.store.requests().is_empty());
}

#[tokio::test]
async fn until_picks_the_days_walked() {
    let (first, later) = (days_ago(5), days_ago(3));
    let tree = minted_on(first, 5);
    let days = Days::new();
    days.put(first, &[&family(tree, first)]).await;
    days.put(
        later,
        &[&flush(vec![vec![state_row(tree, 0, at(later, 0), None)]])],
    )
    .await;
    let until_ms = Some(noon(first).as_millisecond());

    let until = GetTreeRequest {
        until_ms,
        ..tree_request(tree)
    };
    assert_eq!(self::tree(&days, until).await.len(), 6);
    assert_eq!(tree_items(&days, tree).await.len(), 7);
    let flow_until = GetFlowRequest {
        until_ms,
        ..flow_request(tree, 0)
    };
    assert_eq!(flow(&days, flow_until).await.len(), 6);
}

#[tokio::test]
async fn an_until_on_no_day_is_refused() {
    let (days, tree) = one_tree().await;
    // The first is out of the range of times. The second is a time, but
    // in year -1, which no day can name.
    let cases = [
        (
            i64::MAX,
            "until_ms 9223372036854775807 is out of the range of times",
        ),
        (-62_200_000_000_000, "is out of the range of days"),
    ];
    for (until_ms, expected) in cases {
        let request = GetTreeRequest {
            until_ms: Some(until_ms),
            ..tree_request(tree)
        };
        let error = refused(api(&days).get_tree(Request::new(request)).await);
        assert!(error.contains(expected), "{until_ms}: {error}");
    }
}

#[tokio::test]
async fn a_limit_ends_the_answer_with_a_message_that_tells_it_was_cut() {
    let (days, tree) = one_tree().await;

    let tree_items = self::tree(
        &days,
        GetTreeRequest {
            limit: Some(2),
            ..tree_request(tree)
        },
    )
    .await;
    let flow_items = flow(
        &days,
        GetFlowRequest {
            limit: Some(1),
            ..flow_request(tree, 1)
        },
    )
    .await;

    assert_eq!(subflows(&tree_items), [0, 0]);
    assert!(
        matches!(tree_items.last(), Some(Item::Truncated(_))),
        "{tree_items:?}"
    );
    assert_eq!(subflows(&flow_items), [1]);
    assert!(
        matches!(flow_items.last(), Some(Item::Truncated(_))),
        "{flow_items:?}"
    );
}

#[tokio::test]
async fn a_limit_the_service_does_not_serve_is_refused() {
    let (days, tree) = one_tree().await;
    for (limit, expected) in [
        (10_001, "limit 10001 is over the maximum of 10000"),
        (0, "limit must be at least 1"),
    ] {
        let request = GetTreeRequest {
            limit: Some(limit),
            ..tree_request(tree)
        };
        let error = refused(api(&days).get_tree(Request::new(request)).await);
        assert_eq!(error, expected);
        let request = GetFlowRequest {
            limit: Some(limit),
            ..flow_request(tree, 0)
        };
        let error = refused(api(&days).get_flow(Request::new(request)).await);
        assert_eq!(error, expected);
    }
}

#[tokio::test]
async fn an_order_or_a_cursor_is_refused_until_served() {
    let (days, tree) = one_tree().await;
    let options = [
        (Order::TsAscending as i32, None),
        (42, None),
        (Order::Unspecified as i32, Some("AAAA".to_string())),
    ];
    for (order, cursor) in options {
        let request = GetTreeRequest {
            order,
            cursor: cursor.clone(),
            ..tree_request(tree)
        };
        let error = refused(api(&days).get_tree(Request::new(request)).await);
        assert!(
            error.contains("not served yet"),
            "{order} {cursor:?}: {error}"
        );
        let request = GetFlowRequest {
            order,
            cursor: cursor.clone(),
            ..flow_request(tree, 0)
        };
        let error = refused(api(&days).get_flow(Request::new(request)).await);
        assert!(
            error.contains("not served yet"),
            "{order} {cursor:?}: {error}"
        );
    }
}

#[tokio::test]
async fn an_empty_cursor_is_no_cursor() {
    let (days, tree) = one_tree().await;
    let request = GetTreeRequest {
        cursor: Some(String::new()),
        ..tree_request(tree)
    };

    assert_eq!(self::tree(&days, request).await.len(), 6);
}

#[tokio::test]
async fn what_the_lookup_could_not_read_comes_before_the_rows() {
    let (days, tree) = one_tree().await;
    let broken = days_ago(2);
    days.put_object(
        &ObjectName::Manifest { day: broken },
        b"not a manifest".to_vec(),
    )
    .await;

    let items = tree_items(&days, tree).await;

    let Some(Item::Problem(problem)) = items.first() else {
        panic!("a problem first: {items:?}");
    };
    assert!(
        problem
            .message
            .starts_with(&format!("the manifest of {broken} is invalid: ")),
        "{problem:?}"
    );
    assert_eq!(rows(&items).len(), 6);
}

#[tokio::test]
async fn states_and_references_are_not_served_yet() {
    let days = Days::new();

    let states = api(&days)
        .list_states(Request::new(ListStatesRequest::default()))
        .await
        .expect_err("a refusal");
    let refs = api(&days)
        .list_refs(Request::new(ListRefsRequest::default()))
        .await
        .expect_err("a refusal");

    assert_eq!(
        (states.code(), refs.code()),
        (Code::Unimplemented, Code::Unimplemented)
    );
}
