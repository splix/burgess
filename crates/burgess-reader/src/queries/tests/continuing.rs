//! A cursor continues the answer it was given for, or is refused.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use burgess_api::Page;
use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::query_service_server::QueryService;
use burgess_api::grpc::{GetTreeRequest, ListRefsRequest, ListStatesRequest, Order};
use burgess_core::flow::TreeId;
use burgess_core::storage::{Day, ObjectName};
use burgess_core::wire::UnixMillis;
use jiff::{SignedDuration, Timestamp};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use tonic::Code;
use tower::ServiceExt;

use super::{RequestError, reference};
use crate::cursor::{Cursor, Position, QueryHash, RowPosition, Window};
use crate::grpc::QueryApi;
use crate::test_support::{Days, at, days_ago, flush, minted_on, state_row};

/// The time `ago` before now, in unix milliseconds.
fn before_now(ago: SignedDuration) -> u64 {
    UnixMillis::from(Timestamp::now() - ago).into()
}

fn ascending_states(limit: u32, cursor: Option<String>) -> ListStatesRequest {
    ListStatesRequest {
        service: "checkout".into(),
        category: "step".into(),
        order: Order::TsAscending.into(),
        limit: Some(limit),
        cursor,
        ..Default::default()
    }
}

/// The window without an end ends at the "now" of the first page. A state
/// stored later, at a time after it, is in no page, as it was not in the
/// answer when it started. The next page is asked of another reader, which
/// knows only the cursor.
#[tokio::test]
async fn the_default_window_does_not_move_between_pages() {
    let day = days_ago(0);
    let tree = minted_on(day, 5);
    let earlier = vec![
        state_row(tree, 0, before_now(SignedDuration::from_mins(10)), None),
        state_row(tree, 1, before_now(SignedDuration::from_mins(5)), None),
    ];
    let days = Days::new();
    days.put(day, &[&flush(vec![earlier.clone()])]).await;

    let first = Page::from_responses(
        days.queries()
            .list_states(ascending_states(1, None))
            .await
            .unwrap(),
    );
    // A time after the "now" of the first page, which is in whole
    // milliseconds.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let later = state_row(tree, 2, UnixMillis::from(Timestamp::now()).into(), None);
    days.put(day, &[&flush(vec![earlier]), &flush(vec![vec![later]])])
        .await;
    let rest = Page::from_responses(
        days.queries()
            .list_states(ascending_states(10, first.cursor.clone()))
            .await
            .unwrap(),
    );
    let again = Page::from_responses(
        days.queries()
            .list_states(ascending_states(10, None))
            .await
            .unwrap(),
    );

    let subflows =
        |page: &Page| -> Vec<u32> { page.rows.iter().map(|row| row.subflow.into()).collect() };
    assert_eq!(subflows(&first), [0]);
    assert_eq!(subflows(&rest), [1], "the window of the first page");
    assert_eq!(subflows(&again), [0, 1, 2], "a new answer has a new window");
}

/// A tree of two days, and the cursor after its first row.
async fn cut_tree() -> (Days, TreeId, String) {
    let (first, second) = (days_ago(3), days_ago(2));
    let tree = minted_on(first, 5);
    let days = Days::new();
    days.put(
        first,
        &[&flush(vec![vec![
            state_row(tree, 0, at(first, 0), None),
            state_row(tree, 0, at(first, 1), None),
        ]])],
    )
    .await;
    days.put(
        second,
        &[&flush(vec![vec![state_row(tree, 0, at(second, 0), None)]])],
    )
    .await;
    let first_page = Page::from_responses(
        days.queries()
            .get_tree(tree_request(tree, Some(1), None))
            .await
            .unwrap(),
    );
    let cursor = first_page.cursor.expect("a cursor after the first row");
    (days, tree, cursor)
}

fn tree_request(tree: TreeId, limit: Option<u32>, cursor: Option<String>) -> GetTreeRequest {
    GetTreeRequest {
        tree: Some(Tree::TreeId(tree.to_string())),
        limit,
        cursor,
        ..Default::default()
    }
}

/// What is left of `day` after retention: nothing, also no manifest.
async fn purge(days: &Days, day: Day) {
    let manifest = Path::from(ObjectName::Manifest { day }.to_string());
    days.store.inner.delete(&manifest).await.unwrap();
}

#[tokio::test]
async fn a_cursor_into_a_purged_day_is_refused() {
    let (days, tree, cursor) = cut_tree().await;
    purge(&days, days_ago(3)).await;

    let answer = days
        .queries()
        .get_tree(tree_request(tree, None, Some(cursor)))
        .await;

    assert!(matches!(answer, Err(RequestError::Expired)), "{answer:?}");
}

/// A writer that restarts can drop the flushes of a day that is not
/// sealed.
#[tokio::test]
async fn a_cursor_into_a_flush_that_the_writer_dropped_is_refused() {
    let (days, tree, cursor) = cut_tree().await;
    days.put(days_ago(3), &[]).await;

    let answer = days
        .queries()
        .get_tree(tree_request(tree, None, Some(cursor)))
        .await;

    assert!(matches!(answer, Err(RequestError::Expired)), "{answer:?}");
}

/// An expired cursor is not a fault of how the request is written, and a
/// retry does not help, thus each transport tells it in its own way.
#[tokio::test]
async fn an_expired_cursor_has_its_own_status_on_each_transport() {
    let (days, tree, cursor) = cut_tree().await;
    purge(&days, days_ago(3)).await;
    let queries = Arc::new(days.queries());

    let grpc = QueryApi::new(queries.clone())
        .get_tree(tonic::Request::new(tree_request(
            tree,
            None,
            Some(cursor.clone()),
        )))
        .await;
    let rest = crate::rest::router(queries)
        .oneshot(
            Request::get(format!("/v1/trees/{tree}?cursor={cursor}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = grpc.expect_err("a refusal");
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(rest.status(), StatusCode::GONE);
}

/// Only a cursor made outside the service can have a position of another
/// order than its request, since the order is in the hash. It is refused,
/// and does not reach a lookup that reads it as its own kind.
#[tokio::test]
async fn a_cursor_of_another_kind_of_answer_is_refused() {
    let (days, tree, _) = cut_tree().await;
    let request = tree_request(tree, None, None);
    let made = Cursor {
        window: Window::new(None, None).unwrap(),
        position: Position::Ordered {
            ts: UnixMillis::from(at(days_ago(3), 0)),
            row: RowPosition {
                day: days_ago(3),
                nnnn: UnixMillis::from(at(days_ago(3), 0)),
                index: 0,
            },
        },
        oldest: days_ago(3),
    }
    .encode(QueryHash::of(&request));

    let answer = days
        .queries()
        .get_tree(GetTreeRequest {
            cursor: Some(made),
            ..request
        })
        .await;

    assert!(
        matches!(answer, Err(RequestError::CursorMismatch)),
        "{answer:?}"
    );
}

/// A position in the file of `cut_tree`, which is still stored.
fn stored_row() -> RowPosition {
    RowPosition {
        day: days_ago(3),
        nnnn: UnixMillis::from(at(days_ago(3), 0)),
        index: 0,
    }
}

/// As for a tree, a ListStates cursor of another kind than its request is
/// refused: a position of an answer as read, or a window without a start,
/// which no ListStates answer gives.
#[tokio::test]
async fn a_states_cursor_of_another_kind_of_answer_is_refused() {
    let (days, _, _) = cut_tree().await;
    let request = ascending_states(10, None);
    let until = Timestamp::now().round(jiff::Unit::Millisecond).unwrap();
    let since = until - SignedDuration::from_hours(24);
    let as_read = Cursor {
        window: Window::new(Some(since), Some(until)).unwrap(),
        position: Position::Unordered(stored_row()),
        oldest: days_ago(3),
    };
    let no_start = Cursor {
        window: Window::new(None, Some(until)).unwrap(),
        position: Position::Ordered {
            ts: UnixMillis::from(until),
            row: stored_row(),
        },
        oldest: days_ago(3),
    };

    for made in [as_read, no_start] {
        let cursor = made.encode(QueryHash::of(&request));
        let answer = days
            .queries()
            .list_states(ListStatesRequest {
                cursor: Some(cursor),
                ..request.clone()
            })
            .await;

        assert!(
            matches!(answer, Err(RequestError::CursorMismatch)),
            "{made:?}: {answer:?}"
        );
    }
}

/// The service trims the reference, thus a request that differs only in
/// what it trims asks for the same answer, and continues it.
#[tokio::test]
async fn a_references_cursor_continues_a_request_that_differs_only_in_spaces() {
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
    let request = |ref_type: &str, domain_id: &str, cursor: Option<String>| ListRefsRequest {
        ref_type: ref_type.into(),
        domain_id: domain_id.into(),
        limit: Some(1),
        cursor,
        ..Default::default()
    };
    let first = Page::from_responses(
        days.queries()
            .list_refs(request(" order_id", "A ", None))
            .await
            .unwrap(),
    );

    let rest = days
        .queries()
        .list_refs(request("order_id", "A", first.cursor))
        .await;

    let rest = Page::from_responses(rest.expect("the same answer"));
    assert_eq!(rest.rows.len(), 1);
}
