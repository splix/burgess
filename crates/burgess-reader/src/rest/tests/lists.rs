//! The states and references routes: GET and POST read the same options,
//! and answer as gRPC does.

use axum::http::header::CONTENT_TYPE;
use burgess_api::RefRow;
use burgess_api::grpc::{ListRefsRequest, ListStatesRequest, Order};
use burgess_core::storage::StateEntry;
use jiff::Timestamp;
use serde_json::{Value, json};

use super::*;
use crate::test_support::state_with_payload;

async fn post(router: Router, uri: &str, body: &Value) -> (StatusCode, Vec<u8>) {
    let request = Request::post(uri)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, body.to_vec())
}

fn read<T: DeserializeOwned>(status: StatusCode, (got, body): (StatusCode, Vec<u8>)) -> T {
    assert_eq!(got, status, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

/// Four states of two days ago, `n` ms after noon, with the amount `10 * n`
/// and the region `eu` for even `n`.
async fn some_states() -> (Days, Day) {
    let day = days_ago(2);
    let tree = minted_on(day, 5);
    let days = Days::new();
    let rows = (0..4)
        .map(|n| {
            let payload = format!(r#"{{"amount": {}}}"#, 10 * n);
            let mut row = state_with_payload(tree, n, at(day, u64::from(n)), payload.as_bytes());
            if let StateEntry::State { meta, .. } = &mut row.entry {
                let region = if n % 2 == 0 { "eu" } else { "us" };
                meta.insert("region".into(), region.into());
            }
            row
        })
        .collect();
    days.put(day, &[&flush(vec![rows])]).await;
    (days, day)
}

/// The amount of each state of `page`.
fn amounts(page: &Value) -> Vec<i64> {
    page["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["payload"]["amount"].as_i64().unwrap())
        .collect()
}

#[tokio::test]
async fn states_by_get_and_by_post_answer_the_same_page_as_grpc() {
    let (days, day) = some_states().await;
    let since = at(day, 0);
    let until = Timestamp::from_millisecond(at(day, 100) as i64).unwrap();
    let query = format!(
        "/v1/states?service=checkout&category=step&meta.region=eu&where=amount%20%3E%3D%200\
         &since={since}&until={until}&order=desc&limit=5"
    );
    let body = json!({
        "service": "checkout",
        "category": "step",
        "meta": {"region": "eu"},
        "where": "amount >= 0",
        "since": since,
        "until": until.to_string(),
        "order": "desc",
        "limit": 5,
    });
    let grpc = days
        .queries()
        .list_states(ListStatesRequest {
            service: "checkout".into(),
            category: "step".into(),
            meta: [("region".to_string(), "eu".to_string())].into(),
            r#where: Some("amount >= 0".into()),
            since_ms: Some(since as i64),
            until_ms: Some(until.as_millisecond()),
            order: Order::TsDescending as i32,
            limit: Some(5),
            cursor: None,
        })
        .await
        .unwrap();

    let by_get: Value = read(StatusCode::OK, get(router_over(&days), &query).await);
    let by_post: Value = read(
        StatusCode::OK,
        post(router_over(&days), "/v1/states", &body).await,
    );

    assert_eq!(amounts(&by_get), [20, 0]);
    assert_eq!(by_get, by_post);
    assert_eq!(
        by_get,
        serde_json::to_value(Page::from_responses(grpc)).unwrap()
    );
    assert_eq!(by_get["rows"][0]["kind"], "state");
}

#[tokio::test]
async fn states_can_be_ordered_by_time_both_ways() {
    let (days, day) = some_states().await;
    let window = format!(
        "/v1/states?service=checkout&category=step&since={}&until={}",
        at(day, 0),
        at(day, 100)
    );

    let ascending: Value = read(
        StatusCode::OK,
        get(router_over(&days), &format!("{window}&order=asc")).await,
    );
    let descending: Value = read(
        StatusCode::OK,
        get(router_over(&days), &format!("{window}&order=desc&limit=2")).await,
    );

    assert_eq!(amounts(&ascending), [0, 10, 20, 30]);
    assert_eq!(amounts(&descending), [30, 20]);
    assert!(descending["cursor"].is_string(), "{descending}");
}

#[tokio::test]
async fn options_that_cannot_be_read_are_refused() {
    let days = Days::new();
    let cases = [
        (
            "/v1/states?service=a&category=b&meta.k=1&meta.k=2",
            "meta.k is given more than one time",
        ),
        (
            "/v1/states?service=a&service=b&category=c",
            "service is given more than one time",
        ),
        (
            "/v1/states?service=a&category=b&servce=c",
            "invalid options: unknown field `servce`",
        ),
        (
            "/v1/states?service=a&category=b&meta=eu&meta.region=us",
            "meta is given more than one time",
        ),
        (
            "/v1/trees/0102030405060708090a0b0c?order=asc",
            "only ListStates serves an order",
        ),
        (
            "/v1/states?service=a&category=b&order=up",
            "invalid order \"up\", expected asc or desc",
        ),
        (
            "/v1/states?service=a&category=b&since=yesterday",
            "invalid since: \"yesterday\" is not an RFC 3339 time or unix milliseconds",
        ),
        (
            "/v1/states?service=a&category=b&limit=many",
            "invalid limit \"many\"",
        ),
        ("/v1/states?category=b", "service is necessary"),
        (
            "/v1/states?service=a&category=b&where=amount%20%3E",
            "invalid where: expected a string, a number, `true`, `false` or `null` at column 9",
        ),
        (
            "/v1/refs?ref_type=order_id&domain_id=ORD-1&meta.k=1",
            "unknown field `meta`",
        ),
        (
            "/v1/refs?ref_type=order_id&domain_id=ORD-1&order=asc",
            "only ListStates serves an order",
        ),
    ];
    for (uri, expected) in cases {
        let error = refused(router_over(&days), uri).await;
        assert!(error.contains(expected), "{uri}: {error}");
    }
    assert!(
        days.store.requests().is_empty(),
        "a refused request reads nothing"
    );
}

#[tokio::test]
async fn a_body_that_cannot_be_read_is_refused_as_the_query_would_be() {
    let days = Days::new();
    let cases = [
        (
            json!({"service": "a", "category": "b", "colour": "red"}),
            "/v1/states?service=a&category=b&colour=red",
        ),
        (
            json!({"service": "a", "category": "b", "limit": "many"}),
            "/v1/states?service=a&category=b&limit=many",
        ),
    ];
    for (body, query) in cases {
        let by_body: ErrorBody = read(
            StatusCode::BAD_REQUEST,
            post(router_over(&days), "/v1/states", &body).await,
        );
        let by_query = refused(router_over(&days), query).await;
        assert_eq!(by_body.error, by_query, "{body}");
    }
}

#[tokio::test]
async fn a_body_value_of_a_wrong_type_is_refused_with_what_the_option_takes() {
    let days = Days::new();
    let cases = [
        (json!({"limit": -1}), "expected a number of rows"),
        (json!({"limit": 1.5}), "expected a number of rows"),
        (
            json!({"since": true}),
            "expected unix milliseconds or an RFC 3339 time",
        ),
        (
            json!({"meta": ["region"]}),
            "invalid options: invalid type: sequence",
        ),
        (
            json!(["checkout", "step"]),
            "invalid options: expected a JSON object of options",
        ),
    ];
    for (body, expected) in cases {
        let error: ErrorBody = read(
            StatusCode::BAD_REQUEST,
            post(router_over(&days), "/v1/states", &body).await,
        );
        assert!(error.error.contains(expected), "{body}: {}", error.error);
    }
    let request = Request::post("/v1/states").body(Body::from("{}")).unwrap();
    let response = router_over(&days).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

/// Two references to ORD-1 and one to ORD-2, two days ago.
async fn some_references() -> (Days, Day, TreeId) {
    let day = days_ago(2);
    let tree = minted_on(day, 5);
    let days = Days::new();
    let reference = |domain_id: &str, subflow: u32| burgess_core::storage::RefRow {
        ref_type: "order_id".into(),
        domain_id: domain_id.into(),
        service: "checkout".into(),
        category: Some("payment".into()),
        tree,
        subflow: Subflow::from(subflow),
        time: UnixMillis::from(at(day, u64::from(subflow))),
    };
    days.put_refs(
        day,
        &[&[
            reference("ORD-1", 0),
            reference("ORD-2", 1),
            reference("ORD-1", 2),
        ]],
    )
    .await;
    (days, day, tree)
}

#[tokio::test]
async fn references_by_get_and_by_post_answer_the_same_page_as_grpc() {
    let (days, day, tree) = some_references().await;
    let since = at(day, 0);
    let query =
        format!("/v1/refs?ref_type=order_id&domain_id=ORD-1&service=checkout&since={since}");
    let body = json!({
        "ref_type": "order_id",
        "domain_id": "ORD-1",
        "service": "checkout",
        "since": since,
    });
    let grpc = days
        .queries()
        .list_refs(ListRefsRequest {
            ref_type: "order_id".into(),
            domain_id: "ORD-1".into(),
            service: Some("checkout".into()),
            since_ms: Some(since as i64),
            ..Default::default()
        })
        .await
        .unwrap();

    let by_get: Page<RefRow> = read(StatusCode::OK, get(router_over(&days), &query).await);
    let by_post: Value = read(
        StatusCode::OK,
        post(router_over(&days), "/v1/refs", &body).await,
    );

    assert_eq!(by_get.rows.len(), 2);
    assert_eq!(by_get.rows[0].tree, tree);
    assert_eq!(by_get.rows[0].category.as_deref(), Some("payment"));
    assert_eq!(serde_json::to_value(&by_get).unwrap(), by_post);
    assert_eq!(
        by_post,
        serde_json::to_value(Page::from_responses(grpc)).unwrap()
    );
}

#[tokio::test]
async fn an_outside_id_with_any_character_can_be_given_in_the_query() {
    let day = days_ago(2);
    let tree = minted_on(day, 5);
    let days = Days::new();
    let odd = "A&B/C+D%E F";
    days.put_refs(
        day,
        &[&[burgess_core::storage::RefRow {
            ref_type: "order_id".into(),
            domain_id: odd.into(),
            service: "checkout".into(),
            category: None,
            tree,
            subflow: Subflow::ROOT,
            time: UnixMillis::from(at(day, 0)),
        }]],
    )
    .await;

    let page: Page<RefRow> = read(
        StatusCode::OK,
        get(
            router_over(&days),
            "/v1/refs?ref_type=order_id&domain_id=A%26B%2FC%2BD%25E%20F",
        )
        .await,
    );

    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].domain_id, odd);
}

#[tokio::test]
async fn a_where_that_breaks_the_grammar_tells_its_column_in_the_body() {
    let days = Days::new();

    let by_query: ErrorBody = get_json(
        router_over(&days),
        "/v1/states?service=a&category=b&where=%20%20amount%20%3E",
        StatusCode::BAD_REQUEST,
    )
    .await;
    let other: ErrorBody = get_json(
        router_over(&days),
        "/v1/states?service=a&category=b&limit=0",
        StatusCode::BAD_REQUEST,
    )
    .await;

    assert_eq!(by_query.where_column, Some(11));
    assert_eq!(other.where_column, None);
}
