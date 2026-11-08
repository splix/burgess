//! Calls to the gRPC service on days of real files: the messages of the
//! answers, and the requests that the service refuses.

use std::sync::Arc;

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::query_service_server::QueryService;
use burgess_api::grpc::{
    GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest, Order, PayloadType,
    get_flow_response, get_tree_response::Item, list_refs_response, list_states_response,
};
use burgess_api::{Row, RowKind};
use burgess_core::flow::{FlowId, Subflow, TreeId};
use burgess_core::storage::{Day, ObjectName, RefRow, StateEntry, StateRow};
use burgess_core::wire::UnixMillis;
use jiff::{SignedDuration, Timestamp};
use tonic::{Code, Request, Status};

use super::*;
use crate::test_support::{
    Days, at, days_ago, flush, messages, minted_on, noon, state_row, state_with_payload,
};

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
    QueryApi::new(Arc::new(days.queries()))
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

async fn tree(days: &Days, request: GetTreeRequest) -> Vec<Item> {
    let answer = api(days).get_tree(Request::new(request)).await;
    messages(answer)
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
    messages(answer)
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
async fn an_order_or_a_cursor_is_refused_for_a_tree() {
    let (days, tree) = one_tree().await;
    let options = [
        (
            Order::TsAscending as i32,
            None,
            "only ListStates serves an order",
        ),
        (42, None, "unknown order 42"),
        (
            Order::Unspecified as i32,
            Some("AAAA".to_string()),
            "cursors are not served yet",
        ),
        (
            Order::TsAscending as i32,
            Some("AAAA".to_string()),
            "only ListStates serves an order",
        ),
    ];
    for (order, cursor, expected) in options {
        let request = GetTreeRequest {
            order,
            cursor: cursor.clone(),
            ..tree_request(tree)
        };
        let error = refused(api(&days).get_tree(Request::new(request)).await);
        assert!(error.contains(expected), "{order} {cursor:?}: {error}");
        let request = GetFlowRequest {
            order,
            cursor: cursor.clone(),
            ..flow_request(tree, 0)
        };
        let error = refused(api(&days).get_flow(Request::new(request)).await);
        assert!(error.contains(expected), "{order} {cursor:?}: {error}");
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

/// A request for the states that [`state_row`] makes.
fn states_request() -> ListStatesRequest {
    ListStatesRequest {
        service: "checkout".into(),
        category: "step".into(),
        ..Default::default()
    }
}

async fn states(days: &Days, request: ListStatesRequest) -> Vec<list_states_response::Item> {
    let answer = api(days).list_states(Request::new(request)).await;
    messages(answer)
        .await
        .into_iter()
        .map(|message| message.item.expect("an item"))
        .collect()
}

/// `ago` before now, in unix milliseconds.
fn before_now(ago: SignedDuration) -> u64 {
    (Timestamp::now() - ago).as_millisecond() as u64
}

/// Stores `rows` as one flush on the day of their first time.
async fn put_states(days: &Days, rows: Vec<StateRow>) {
    let day = Day::utc(rows[0].time.saturating_timestamp()).unwrap();
    days.put(day, &[&flush(vec![rows])]).await;
}

/// The state rows of `items`.
fn state_rows(
    items: &[list_states_response::Item],
) -> impl Iterator<Item = &burgess_api::grpc::StateRow> {
    items.iter().filter_map(|item| match item {
        list_states_response::Item::Row(row) => Some(row),
        _ => None,
    })
}

/// The time of each state row of `items`.
fn state_times(items: &[list_states_response::Item]) -> Vec<i64> {
    state_rows(items).map(|row| row.ts_ms).collect()
}

#[tokio::test]
async fn states_answer_with_a_message_per_state() {
    let ms = before_now(SignedDuration::from_hours(1));
    let tree = minted_on(days_ago(0), 5);
    let mut row = state_with_payload(tree, 2, ms, br#" {"amount": 10} "#);
    if let StateEntry::State { meta, .. } = &mut row.entry {
        *meta = [("region".to_string(), "eu".to_string())].into();
    }
    let days = Days::new();
    put_states(&days, vec![row, state_row(tree, 2, ms + 1, Some(3))]).await;

    let items = states(&days, states_request()).await;

    assert_eq!(
        items,
        [list_states_response::Item::Row(
            burgess_api::grpc::StateRow {
                flow_id: FlowId::new(tree, Subflow::from(2)).to_string(),
                tree: tree.to_string(),
                subflow: 2,
                ts_ms: ms as i64,
                service: "checkout".into(),
                category: "step".into(),
                meta: [("region".to_string(), "eu".to_string())].into(),
                payload_type: PayloadType::Json as i32,
                payload: Some(r#"{"amount": 10}"#.into()),
            }
        )],
        "the split is not a state"
    );
}

#[tokio::test]
async fn without_since_the_window_is_the_day_before_until() {
    let tree = minted_on(days_ago(0), 5);
    let recent = before_now(SignedDuration::from_hours(1));
    let old = before_now(SignedDuration::from_hours(25));
    let days = Days::new();
    put_states(&days, vec![state_row(tree, 0, recent, None)]).await;
    put_states(&days, vec![state_row(tree, 1, old, None)]).await;

    let default = states(&days, states_request()).await;
    let longer = states(
        &days,
        ListStatesRequest {
            since_ms: Some(before_now(SignedDuration::from_hours(26)) as i64),
            ..states_request()
        },
    )
    .await;
    let ending_earlier = states(
        &days,
        ListStatesRequest {
            until_ms: Some(before_now(SignedDuration::from_hours(2)) as i64),
            ..states_request()
        },
    )
    .await;

    assert_eq!(state_times(&default), [recent as i64]);
    assert_eq!(state_times(&longer), [recent as i64, old as i64]);
    assert_eq!(state_times(&ending_earlier), [old as i64]);
}

#[tokio::test]
async fn a_payload_that_is_not_json_comes_with_a_problem_before_its_state() {
    let ms = before_now(SignedDuration::from_hours(1));
    let row = state_with_payload(minted_on(days_ago(0), 5), 0, ms, b"not json");
    let days = Days::new();
    put_states(&days, vec![row]).await;

    let items = states(&days, states_request()).await;

    let [
        list_states_response::Item::Problem(problem),
        list_states_response::Item::Row(row),
    ] = items.as_slice()
    else {
        panic!("a problem, then a row: {items:?}");
    };
    assert!(problem.message.contains("is not valid JSON"), "{problem:?}");
    assert_eq!(row.payload, None);
}

#[tokio::test]
async fn a_limit_ends_the_states_with_a_message_that_tells_they_were_cut() {
    let ms = before_now(SignedDuration::from_hours(1));
    let tree = minted_on(days_ago(0), 5);
    let days = Days::new();
    put_states(
        &days,
        (0..3).map(|i| state_row(tree, i, ms, None)).collect(),
    )
    .await;

    let items = states(
        &days,
        ListStatesRequest {
            limit: Some(2),
            ..states_request()
        },
    )
    .await;

    assert_eq!(state_times(&items).len(), 2);
    assert!(
        matches!(items.last(), Some(list_states_response::Item::Truncated(_))),
        "{items:?}"
    );
}

#[tokio::test]
async fn where_keeps_the_states_whose_payload_passes() {
    let ms = before_now(SignedDuration::from_hours(1));
    let tree = minted_on(days_ago(0), 5);
    let with_amount =
        |subflow, payload: &str| state_with_payload(tree, subflow, ms, payload.as_bytes());
    let days = Days::new();
    put_states(
        &days,
        vec![
            with_amount(0, r#"{"amount": 50}"#),
            with_amount(1, r#"{"amount": 150}"#),
            with_amount(2, r#"{"amount": "150"}"#),
            with_amount(3, r#"{"amount": 250.5}"#),
        ],
    )
    .await;

    let items = states(
        &days,
        ListStatesRequest {
            r#where: Some(" amount > 100 ".into()),
            ..states_request()
        },
    )
    .await;

    let subflows: Vec<u32> = state_rows(&items).map(|row| row.subflow).collect();
    assert_eq!(subflows, [1, 3], "{items:?}");
}

#[tokio::test]
async fn the_limit_counts_the_states_that_pass_where() {
    let ms = before_now(SignedDuration::from_hours(1));
    let tree = minted_on(days_ago(0), 5);
    let days = Days::new();
    put_states(
        &days,
        (0..4)
            .map(|subflow| {
                state_with_payload(
                    tree,
                    subflow,
                    ms,
                    format!(r#"{{"n": {subflow}}}"#).as_bytes(),
                )
            })
            .collect(),
    )
    .await;

    let items = states(
        &days,
        ListStatesRequest {
            r#where: Some("n >= 2".into()),
            limit: Some(1),
            ..states_request()
        },
    )
    .await;

    let [
        list_states_response::Item::Row(row),
        list_states_response::Item::Truncated(_),
    ] = items.as_slice()
    else {
        panic!("one row, then truncated: {items:?}");
    };
    assert_eq!(row.subflow, 2);
}

#[tokio::test]
async fn a_blank_where_is_no_filter() {
    let ms = before_now(SignedDuration::from_hours(1));
    let days = Days::new();
    put_states(
        &days,
        vec![state_row(minted_on(days_ago(0), 5), 0, ms, None)],
    )
    .await;

    for blank in ["", " \t"] {
        let items = states(
            &days,
            ListStatesRequest {
                r#where: Some(blank.into()),
                cursor: Some(String::new()),
                ..states_request()
            },
        )
        .await;

        assert_eq!(state_times(&items), [ms as i64], "{blank:?}");
    }
}

#[tokio::test]
async fn a_states_request_the_service_cannot_serve_is_refused() {
    let days = Days::new();
    let cases = [
        (
            ListStatesRequest {
                service: String::new(),
                ..states_request()
            },
            "service is necessary",
        ),
        (
            ListStatesRequest {
                category: String::new(),
                ..states_request()
            },
            "category is necessary",
        ),
        (
            ListStatesRequest {
                r#where: Some("amount >".into()),
                ..states_request()
            },
            "invalid where: expected a string, a number, `true`, `false` or `null` at column 9",
        ),
        (
            ListStatesRequest {
                r#where: Some("  amount >".into()),
                ..states_request()
            },
            "at column 11",
        ),
        (
            ListStatesRequest {
                since_ms: Some(2_000),
                until_ms: Some(1_000),
                ..states_request()
            },
            "is after until",
        ),
        (
            ListStatesRequest {
                until_ms: Some(i64::MIN),
                ..states_request()
            },
            "until_ms -9223372036854775808 is out of the range of times",
        ),
        (
            ListStatesRequest {
                order: 42,
                ..states_request()
            },
            "unknown order 42",
        ),
        (
            ListStatesRequest {
                cursor: Some("next".into()),
                ..states_request()
            },
            "cursors are not served yet",
        ),
        (
            ListStatesRequest {
                limit: Some(0),
                ..states_request()
            },
            "limit must be at least 1",
        ),
    ];
    for (request, expected) in cases {
        let error = refused(api(&days).list_states(Request::new(request.clone())).await);
        assert!(error.contains(expected), "{request:?}: {error}");
    }
    assert!(
        days.store.requests().is_empty(),
        "a refused request reads nothing"
    );
}

#[tokio::test]
async fn what_the_states_lookup_could_not_read_comes_before_the_states() {
    let ms = before_now(SignedDuration::from_hours(1));
    let days = Days::new();
    put_states(
        &days,
        vec![state_row(minted_on(days_ago(0), 5), 0, ms, None)],
    )
    .await;
    days.put_object(
        &ObjectName::Manifest { day: days_ago(-1) },
        b"not a manifest".to_vec(),
    )
    .await;

    let items = states(&days, states_request()).await;

    assert!(
        matches!(
            items.as_slice(),
            [
                list_states_response::Item::Problem(_),
                list_states_response::Item::Row(_),
            ]
        ),
        "{items:?}"
    );
}

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

fn refs_request(domain_id: &str) -> ListRefsRequest {
    ListRefsRequest {
        ref_type: "order_id".into(),
        domain_id: domain_id.into(),
        ..Default::default()
    }
}

async fn refs(days: &Days, request: ListRefsRequest) -> Vec<list_refs_response::Item> {
    let answer = api(days).list_refs(Request::new(request)).await;
    messages(answer)
        .await
        .into_iter()
        .map(|message| message.item.expect("an item"))
        .collect()
}

/// A store with three references to ORD-1 and one to ORD-2, three days
/// ago.
async fn some_references() -> (Days, Day, TreeId) {
    let day = days_ago(3);
    let tree = minted_on(day, 5);
    let days = Days::new();
    days.put_refs(
        day,
        &[&[
            reference("ORD-1", tree, 0, day, 0),
            reference("ORD-1", tree, 1, day, 1),
            reference("ORD-2", tree, 2, day, 2),
            reference("ORD-1", tree, 3, day, 3),
        ]],
    )
    .await;
    (days, day, tree)
}

#[tokio::test]
async fn references_answer_with_a_message_per_reference() {
    let (days, day, tree) = some_references().await;

    let items = refs(&days, refs_request("ORD-1")).await;

    assert_eq!(items.len(), 3, "rows only: {items:?}");
    let list_refs_response::Item::Row(row) = &items[0] else {
        panic!("a row expected: {items:?}");
    };
    assert_eq!(
        *row,
        burgess_api::grpc::RefRow {
            ref_type: "order_id".into(),
            domain_id: "ORD-1".into(),
            service: "checkout".into(),
            category: Some("payment".into()),
            flow_id: FlowId::new(tree, Subflow::ROOT).to_string(),
            tree: tree.to_string(),
            subflow: 0,
            ts_ms: at(day, 0) as i64,
        }
    );
}

#[tokio::test]
async fn a_limit_ends_the_references_with_a_message_that_tells_they_were_cut() {
    let (days, _, _) = some_references().await;

    let items = refs(
        &days,
        ListRefsRequest {
            limit: Some(2),
            ..refs_request("ORD-1")
        },
    )
    .await;

    assert_eq!(items.len(), 3, "{items:?}");
    assert!(
        matches!(items.last(), Some(list_refs_response::Item::Truncated(_))),
        "{items:?}"
    );
}

#[tokio::test]
async fn an_id_with_spaces_around_it_finds_what_the_writer_stored() {
    let (days, _, _) = some_references().await;

    let items = refs(
        &days,
        ListRefsRequest {
            domain_id: " ORD-2 ".into(),
            ..refs_request("ORD-2")
        },
    )
    .await;

    assert_eq!(items.len(), 1, "{items:?}");
}

#[tokio::test]
async fn an_empty_filter_is_no_filter() {
    let (days, _, _) = some_references().await;

    let items = refs(
        &days,
        ListRefsRequest {
            service: Some(String::new()),
            category: Some(String::new()),
            ..refs_request("ORD-1")
        },
    )
    .await;

    assert_eq!(items.len(), 3, "{items:?}");
}

#[tokio::test]
async fn a_references_request_the_service_cannot_serve_is_refused() {
    let days = Days::new();
    let cases = [
        (
            ListRefsRequest {
                ref_type: "order id".into(),
                ..refs_request("ORD-1")
            },
            "invalid reference: ",
        ),
        (refs_request("  "), "invalid reference: "),
        (
            ListRefsRequest {
                since_ms: Some(2_000),
                until_ms: Some(1_000),
                ..refs_request("ORD-1")
            },
            "is after until",
        ),
        (
            ListRefsRequest {
                since_ms: Some(i64::MAX),
                ..refs_request("ORD-1")
            },
            "since_ms 9223372036854775807 is out of the range of times",
        ),
        (
            ListRefsRequest {
                order: Order::TsDescending as i32,
                ..refs_request("ORD-1")
            },
            "only ListStates serves an order",
        ),
        (
            ListRefsRequest {
                limit: Some(0),
                ..refs_request("ORD-1")
            },
            "limit must be at least 1",
        ),
    ];
    for (request, expected) in cases {
        let error = refused(api(&days).list_refs(Request::new(request.clone())).await);
        assert!(error.contains(expected), "{request:?}: {error}");
    }
    assert!(
        days.store.requests().is_empty(),
        "a refused request reads nothing"
    );
}

#[tokio::test]
async fn what_the_refs_lookup_could_not_read_comes_before_the_references() {
    let (days, _, _) = some_references().await;
    days.put_object(
        &ObjectName::Manifest { day: days_ago(2) },
        b"not a manifest".to_vec(),
    )
    .await;

    let items = refs(&days, refs_request("ORD-1")).await;

    assert!(
        matches!(
            items.as_slice(),
            [
                list_refs_response::Item::Problem(_),
                list_refs_response::Item::Row(_),
                list_refs_response::Item::Row(_),
                list_refs_response::Item::Row(_),
            ]
        ),
        "{items:?}"
    );
}
