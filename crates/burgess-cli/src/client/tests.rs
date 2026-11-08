//! Requests to a mock query service, and how its answers are read.

use std::num::NonZeroUsize;

use burgess_core::report::error_chain;
use jiff::Timestamp;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const TREE: &str = "019a5c2e7f00a1b2c3d4e5f6";
const FLOW: &str = "019a5c2e7f00a1b2c3d4e5f63f2a9c01";

fn page_body() -> serde_json::Value {
    json!({"rows": [{
        "flow_id": format!("{TREE}00000000"), "tree": TREE, "subflow": 0,
        "ts": 1_793_880_000_000_u64, "kind": "state", "service": "checkout",
        "category": "order", "meta": {}, "payload_type": "json",
        "payload": {"order": 7}, "child_subflow": null,
    }], "truncated": false})
}

fn client(server: &MockServer) -> QueryClient {
    QueryClient::new(server.uri().parse().unwrap())
}

fn tree() -> Lookup {
    Lookup::Tree(TREE.parse().unwrap())
}

fn no_options() -> PageArgs {
    PageArgs {
        until: None,
        limit: None,
    }
}

async fn answering(template: ResponseTemplate) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(template)
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn asks_for_a_tree_with_its_options_and_reads_the_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/v1/trees/{TREE}")))
        .and(query_param("until", "1793880000000"))
        .and(query_param("limit", "20"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body()))
        .expect(1)
        .mount(&server)
        .await;
    let options = PageArgs {
        until: Some(Timestamp::from_millisecond(1_793_880_000_000).unwrap()),
        limit: NonZeroUsize::new(20),
    };

    let page = client(&server).page(tree(), options).await.unwrap();

    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        page.rows[0].payload.as_ref().unwrap().get(),
        r#"{"order":7}"#
    );
}

#[tokio::test]
async fn asks_for_a_flow_without_options() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/v1/flows/{FLOW}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body()))
        .expect(1)
        .mount(&server)
        .await;

    let lookup = Lookup::Flow(FLOW.parse().unwrap());
    client(&server).page(lookup, no_options()).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests[0].url.query(), None);
}

#[tokio::test]
async fn a_service_behind_a_path_is_asked_below_it() {
    let server = MockServer::start().await;
    Mock::given(path(format!("/burgess/v1/trees/{TREE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body()))
        .expect(2)
        .mount(&server)
        .await;

    for base in ["burgess", "burgess/"] {
        let client = QueryClient::new(format!("{}/{base}", server.uri()).parse().unwrap());
        client.page(tree(), no_options()).await.unwrap();
    }
}

#[tokio::test]
async fn a_refusal_carries_the_services_own_words() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"error": "limit 20000 is over the maximum of 10000"})),
        )
        .mount(&server)
        .await;

    let error = client(&server)
        .page(tree(), no_options())
        .await
        .unwrap_err();

    assert_eq!(
        error_chain(&error),
        "the query service answered 400 Bad Request: limit 20000 is over the maximum of 10000"
    );
    assert!(error.is_usage());
}

#[tokio::test]
async fn an_error_without_an_error_body_shows_the_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(502).set_body_string("Bad Gateway\n"))
        .mount(&server)
        .await;

    let error = client(&server)
        .page(tree(), no_options())
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "the query service answered 502 Bad Gateway: Bad Gateway"
    );
    assert!(!error.is_usage());
}

#[tokio::test]
async fn an_empty_error_says_there_was_no_message() {
    let server = answering(ResponseTemplate::new(404)).await;

    let error = client(&server)
        .page(tree(), no_options())
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "the query service answered 404 Not Found: no message"
    );
}

#[tokio::test]
async fn an_answer_that_is_not_a_page_is_reported() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>"))
        .mount(&server)
        .await;

    let error = client(&server)
        .page(tree(), no_options())
        .await
        .unwrap_err();

    assert!(matches!(error, ClientError::NotAPage { .. }), "{error:?}");
}

#[tokio::test]
async fn a_service_that_is_not_there_is_reported_with_its_address() {
    let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}/", free.local_addr().unwrap());
    drop(free);

    let error = QueryClient::new(address.parse().unwrap())
        .page(tree(), no_options())
        .await
        .unwrap_err();

    assert!(
        matches!(error, ClientError::Unreachable { .. }),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .starts_with(&format!("asking the query service at {address} failed")),
        "{error}"
    );
}

#[tokio::test]
async fn an_address_without_paths_is_refused_before_asking() {
    let error = QueryClient::new("mailto:burgess@example.com".parse().unwrap())
        .page(tree(), no_options())
        .await
        .unwrap_err();

    assert!(matches!(error, ClientError::NotABase { .. }), "{error:?}");
    assert!(error.is_usage());
}
