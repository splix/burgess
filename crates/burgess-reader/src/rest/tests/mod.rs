//! Requests to the routes over days of real files, checking status, body
//! and the options each route takes.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::query_service_server::QueryService;
use burgess_api::grpc::{GetFlowRequest, GetTreeRequest, TreeResponse};
use burgess_api::{ErrorBody, Page, RowKind};
use burgess_core::flow::{FlowId, Subflow, TreeId};
use burgess_core::storage::{Day, ObjectName, StateRow};
use burgess_core::wire::UnixMillis;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use serde::de::DeserializeOwned;
use tower::ServiceExt;

use super::*;
use crate::grpc::QueryApi;
use crate::test_support::{Days, at, days_ago, flush, messages, minted_on, noon, state_row};

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

fn router_over(days: &Days) -> Router {
    router(Arc::new(days.queries()))
}

async fn get(router: Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let request = Request::get(uri).body(Body::empty()).unwrap();
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, body.to_vec())
}

async fn get_json<T: DeserializeOwned>(router: Router, uri: &str, status: StatusCode) -> T {
    let (got, body) = get(router, uri).await;
    assert_eq!(got, status, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

async fn page(router: Router, uri: &str) -> Page {
    get_json(router, uri, StatusCode::OK).await
}

async fn refused(router: Router, uri: &str) -> String {
    get_json::<ErrorBody>(router, uri, StatusCode::BAD_REQUEST)
        .await
        .error
}

fn subflows(page: &Page) -> Vec<u32> {
    page.rows.iter().map(|row| u32::from(row.subflow)).collect()
}

/// A store with one family tree written three days ago.
async fn one_tree() -> (Days, TreeId) {
    let day = days_ago(3);
    let tree = minted_on(day, 5);
    let days = Days::new();
    days.put(day, &[&family(tree, day)]).await;
    (days, tree)
}

#[tokio::test]
async fn a_tree_answers_with_every_row_in_the_api_shape() {
    let (days, tree) = one_tree().await;

    let page = page(router_over(&days), &format!("/v1/trees/{tree}")).await;

    assert_eq!(subflows(&page), [0, 0, 0, 1, 2, 4]);
    assert!(!page.truncated);
    assert!(page.problems.is_empty());
    let state = &page.rows[0];
    assert_eq!((state.kind, state.tree), (RowKind::State, tree));
    assert_eq!(state.flow_id, FlowId::new(tree, Subflow::ROOT));
    assert_eq!(state.payload.as_ref().unwrap().get(), "{}");
    let split = &page.rows[1];
    assert_eq!(split.kind, RowKind::Split);
    assert_eq!(split.child_subflow, Some(Subflow::from(1)));
    assert!(split.payload.is_none());
}

#[tokio::test]
async fn a_tree_can_be_named_by_any_flow_of_it() {
    let (days, tree) = one_tree().await;
    let flow = FlowId::new(tree, Subflow::from(2));

    let page = page(router_over(&days), &format!("/v1/trees/{flow}")).await;

    assert_eq!(page.rows.len(), 6);
}

#[tokio::test]
async fn a_flow_answers_with_itself_and_the_flows_below_it() {
    let (days, tree) = one_tree().await;
    let flow = FlowId::new(tree, Subflow::from(1));

    let page = page(router_over(&days), &format!("/v1/flows/{flow}")).await;

    assert_eq!(subflows(&page), [1, 2]);
}

#[tokio::test]
async fn a_flow_needs_a_flow_id_not_a_tree_id() {
    let (days, tree) = one_tree().await;

    let error = refused(router_over(&days), &format!("/v1/flows/{tree}")).await;

    assert!(error.starts_with("invalid id: expected"), "{error}");
}

#[tokio::test]
async fn ids_that_are_not_hex_or_of_no_known_length_are_refused() {
    let days = Days::new();
    for route in ["trees", "flows"] {
        let error = refused(router_over(&days), &format!("/v1/{route}/zz")).await;
        assert!(error.contains("hex digits"), "{route}: {error}");
        let error = refused(router_over(&days), &format!("/v1/{route}/0123abcd")).await;
        assert!(error.contains("found 8"), "{route}: {error}");
    }
    assert!(
        days.store.requests().is_empty(),
        "a refused request reads nothing"
    );
}

#[tokio::test]
async fn until_picks_the_days_walked_in_either_form() {
    let (first, later) = (days_ago(5), days_ago(3));
    let tree = minted_on(first, 5);
    let days = Days::new();
    days.put(first, &[&family(tree, first)]).await;
    days.put(
        later,
        &[&flush(vec![vec![state_row(tree, 0, at(later, 0), None)]])],
    )
    .await;
    let millis = noon(first).as_millisecond();

    for until in [millis.to_string(), noon(first).to_string()] {
        let uri = format!("/v1/trees/{tree}?until={until}");
        assert_eq!(page(router_over(&days), &uri).await.rows.len(), 6, "{uri}");
    }
    let all = page(router_over(&days), &format!("/v1/trees/{tree}")).await;
    assert_eq!(all.rows.len(), 7);
}

#[tokio::test]
async fn an_until_that_is_no_time_is_refused() {
    let (days, tree) = one_tree().await;

    let error = refused(
        router_over(&days),
        &format!("/v1/trees/{tree}?until=yesterday"),
    )
    .await;

    assert_eq!(
        error,
        "invalid until: \"yesterday\" is not an RFC 3339 time or unix milliseconds"
    );
}

#[tokio::test]
async fn an_until_on_no_day_is_refused() {
    let (days, tree) = one_tree().await;

    let error = refused(
        router_over(&days),
        &format!("/v1/trees/{tree}?until=-000001-06-01T00:00:00Z"),
    )
    .await;

    assert!(error.contains("is out of the range of days"), "{error}");
}

#[tokio::test]
async fn a_limit_cuts_the_page_and_says_so() {
    let (days, tree) = one_tree().await;

    let page = page(router_over(&days), &format!("/v1/trees/{tree}?limit=2")).await;

    assert_eq!(subflows(&page), [0, 0]);
    assert!(page.truncated);
}

#[tokio::test]
async fn a_limit_the_service_does_not_serve_is_refused() {
    let (days, tree) = one_tree().await;
    let cases = [
        ("10001", "limit 10001 is over the maximum of 10000"),
        ("0", "limit must be at least 1"),
        ("many", "invalid limit \"many\""),
    ];
    for (limit, expected) in cases {
        let uri = format!("/v1/trees/{tree}?limit={limit}");
        let error = refused(router_over(&days), &uri).await;
        assert!(error.starts_with(expected), "{limit}: {error}");
    }
}

#[tokio::test]
async fn an_option_the_route_does_not_take_is_refused_once() {
    let (days, tree) = one_tree().await;

    let error = refused(router_over(&days), &format!("/v1/trees/{tree}?untill=1")).await;

    assert_eq!(
        error.matches("unknown field `untill`").count(),
        1,
        "{error}"
    );
}

#[tokio::test]
async fn a_path_no_route_serves_answers_404_with_an_error_body() {
    let days = Days::new();
    for uri in ["/v1/trees/", "/v1/trees/abc/rows", "/v2/trees/abc"] {
        let error = get_json::<ErrorBody>(router_over(&days), uri, StatusCode::NOT_FOUND).await;
        assert!(error.error.starts_with("no such route"), "{uri}: {error:?}");
    }
}

#[tokio::test]
async fn a_method_other_than_get_answers_405_with_an_error_body() {
    let (days, tree) = one_tree().await;
    let request = Request::post(format!("/v1/trees/{tree}"))
        .body(Body::empty())
        .unwrap();

    let response = router_over(&days).oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert!(response.headers().contains_key(axum::http::header::ALLOW));
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: ErrorBody = serde_json::from_slice(&body).unwrap();
    assert_eq!(error.error, "the route does not serve this method");
}

#[tokio::test]
async fn what_the_lookup_could_not_read_is_listed_with_the_rows() {
    let (days, tree) = one_tree().await;
    let broken = days_ago(2);
    days.put_object(
        &ObjectName::Manifest { day: broken },
        b"not a manifest".to_vec(),
    )
    .await;

    let page = page(router_over(&days), &format!("/v1/trees/{tree}")).await;

    assert_eq!(page.rows.len(), 6);
    assert_eq!(page.problems.len(), 1, "{:?}", page.problems);
    assert!(
        page.problems[0].starts_with(&format!("the manifest of {broken} is invalid: ")),
        "{:?}",
        page.problems
    );
}

#[tokio::test]
async fn a_file_that_is_gone_is_reported_without_repeating_its_cause() {
    let (days, tree) = one_tree().await;
    let day = days_ago(3);
    let gone = ObjectName::StatesFile {
        day,
        nnnn: UnixMillis::from(at(day, 0)),
    };
    days.store
        .inner
        .delete(&Path::from(gone.to_string()))
        .await
        .unwrap();

    let page = page(router_over(&days), &format!("/v1/trees/{tree}")).await;

    let [problem] = page.problems.as_slice() else {
        panic!("one problem expected: {:?}", page.problems);
    };
    assert!(
        problem.starts_with(&format!("reading states from {gone} failed: ")),
        "{problem}"
    );
    let parts: Vec<&str> = problem.split(": ").collect();
    let distinct: std::collections::BTreeSet<&str> = parts.iter().copied().collect();
    assert_eq!(distinct.len(), parts.len(), "a part is repeated: {problem}");
}

/// The JSON page that a gRPC client makes from the answer.
async fn grpc_page<R: TreeResponse>(
    answer: Result<tonic::Response<crate::grpc::Answer<R>>, tonic::Status>,
) -> serde_json::Value {
    serde_json::to_value(Page::from_responses(messages(answer).await)).unwrap()
}

/// REST only translates, thus it gives the same page as gRPC. The cases
/// are the ones that REST translates itself: `until` as RFC 3339, a flow id
/// on the tree route, and the words of a refusal.
#[tokio::test]
async fn rest_and_grpc_answer_with_the_same_page() {
    let (first, later) = (days_ago(5), days_ago(3));
    let tree = minted_on(first, 5);
    let days = Days::new();
    days.put(first, &[&family(tree, first)]).await;
    days.put(
        later,
        &[&flush(vec![vec![state_row(tree, 0, at(later, 0), None)]])],
    )
    .await;
    days.put_object(
        &ObjectName::Manifest { day: days_ago(4) },
        b"not a manifest".to_vec(),
    )
    .await;
    let queries = Arc::new(days.queries());
    let grpc = QueryApi::new(queries.clone());
    let flow = FlowId::new(tree, Subflow::from(1));
    let until = noon(first);

    let rest_tree: serde_json::Value = get_json(
        router(queries.clone()),
        &format!("/v1/trees/{flow}?limit=4&until={until}"),
        StatusCode::OK,
    )
    .await;
    let grpc_tree = grpc_page(
        grpc.get_tree(tonic::Request::new(GetTreeRequest {
            tree: Some(Tree::FlowId(flow.to_string())),
            limit: Some(4),
            until_ms: Some(until.as_millisecond()),
            ..Default::default()
        }))
        .await,
    )
    .await;
    let rest_flow: serde_json::Value = get_json(
        router(queries.clone()),
        &format!("/v1/flows/{flow}"),
        StatusCode::OK,
    )
    .await;
    let grpc_flow = grpc_page(
        grpc.get_flow(tonic::Request::new(GetFlowRequest {
            flow_id: flow.to_string(),
            ..Default::default()
        }))
        .await,
    )
    .await;

    assert_eq!(rest_tree, grpc_tree);
    assert_eq!(rest_tree["truncated"], true, "{rest_tree}");
    assert_eq!(rest_tree["problems"].as_array().map(Vec::len), Some(1));
    assert_eq!(rest_flow, grpc_flow);
    assert_eq!(rest_flow["rows"].as_array().map(Vec::len), Some(2));

    let rest_refusal = refused(router(queries), &format!("/v1/flows/{flow}?limit=0")).await;
    let grpc_refusal = grpc
        .get_flow(tonic::Request::new(GetFlowRequest {
            flow_id: flow.to_string(),
            limit: Some(0),
            ..Default::default()
        }))
        .await
        .expect_err("a refusal");
    assert_eq!(rest_refusal, grpc_refusal.message());
}

#[tokio::test]
async fn a_limit_too_large_for_grpc_is_over_the_maximum() {
    let (days, tree) = one_tree().await;

    let error = refused(
        router_over(&days),
        &format!("/v1/trees/{tree}?limit=4294967296"),
    )
    .await;

    assert_eq!(error, "limit 4294967296 is over the maximum of 10000");
}

#[tokio::test]
async fn both_routes_check_the_id_before_the_options() {
    let days = Days::new();
    for route in ["trees", "flows"] {
        let error = refused(router_over(&days), &format!("/v1/{route}/zz?limit=many")).await;
        assert!(error.starts_with("invalid id"), "{route}: {error}");
    }
}

mod lists;
