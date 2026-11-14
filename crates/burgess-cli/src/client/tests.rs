//! Requests to a gRPC query service in the test process, and how its
//! answers are read.

use std::collections::BTreeMap;

use burgess_api::grpc::tree_row::Entry;
use burgess_api::grpc::{PayloadType, Problem, State, TreeItem, TreeRow};
use burgess_core::report::error_chain;
use jiff::Timestamp;

use super::*;
use crate::args::DaysArgs;
use crate::requests::{PagedRequest, flow_request, tree_request};
use crate::server::ServerAddress;
use crate::test_support::{Answer, MockService, Received};

const TREE: &str = "019a5c2e7f00a1b2c3d4e5f6";
const FLOW: &str = "019a5c2e7f00a1b2c3d4e5f63f2a9c01";

fn state() -> TreeRow {
    TreeRow {
        flow_id: format!("{TREE}00000000"),
        tree: TREE.into(),
        subflow: 0,
        ts_ms: 1_793_880_000_000,
        service: "checkout".into(),
        category: "order".into(),
        entry: Some(Entry::State(State {
            meta: BTreeMap::new(),
            payload_type: PayloadType::Json as i32,
            payload: Some(r#"{"order":7}"#.into()),
        })),
    }
}

fn client(service: &MockService) -> QueryClient {
    QueryClient::new(service.server.clone()).unwrap()
}

fn tree(days: DaysArgs) -> GetTreeRequest {
    tree_request(TREE.parse().unwrap(), days)
}

fn no_options() -> DaysArgs {
    DaysArgs { until: None }
}

async fn lookup_error(answer: Answer) -> (ClientError, ServerAddress) {
    let service = MockService::start(answer).await;
    let error = client(&service).tree(tree(no_options())).await.unwrap_err();
    (error, service.server)
}

#[tokio::test]
async fn asks_for_a_tree_with_its_options_and_reads_the_page() {
    let service = MockService::start(Answer::rows(vec![state()])).await;
    let options = DaysArgs {
        until: Some(Timestamp::from_millisecond(1_793_880_000_000).unwrap()),
    };
    let request = tree(options).page(Some(20), Some("NEXT".into()));

    let reply = client(&service).tree(request).await;
    let page = reply.unwrap().page;

    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        page.rows[0].payload.as_ref().unwrap().get(),
        r#"{"order":7}"#
    );
    assert_eq!(
        service.received(),
        [Received::Tree(GetTreeRequest {
            tree: Some(burgess_api::grpc::get_tree_request::Tree::TreeId(
                TREE.into()
            )),
            until_ms: Some(1_793_880_000_000),
            limit: Some(20),
            cursor: Some("NEXT".into()),
            ..Default::default()
        })]
    );
}

#[tokio::test]
async fn asks_for_a_flow_without_options() {
    let service = MockService::start(Answer::rows(vec![state()])).await;

    let request = flow_request(FLOW.parse().unwrap(), no_options());
    client(&service).flow(request).await.unwrap();

    assert_eq!(
        service.received(),
        [Received::Flow(GetFlowRequest {
            flow_id: FLOW.into(),
            ..Default::default()
        })]
    );
}

#[tokio::test]
async fn problems_and_a_cut_reach_the_page() {
    let service = MockService::start(Answer::cut(
        vec![
            TreeItem::Problem(Problem {
                message: "the manifest of 20261105 is invalid".into(),
            }),
            TreeItem::Row(state()),
        ],
        "NEXT",
    ))
    .await;

    let reply = client(&service).tree(tree(no_options())).await.unwrap();

    let page = &reply.page;
    assert_eq!(page.problems, ["the manifest of 20261105 is invalid"]);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.cursor.as_deref(), Some("NEXT"));
    assert!(!reply.cut_short, "the service ended the stream");
}

#[tokio::test]
async fn a_refusal_carries_the_services_own_words() {
    let (error, _) = lookup_error(Answer::Refuse(Status::invalid_argument(
        "limit 20000 is over the maximum of 10000",
    )))
    .await;

    assert_eq!(
        error_chain(&error),
        "the query service refused the request: limit 20000 is over the maximum of 10000"
    );
    assert!(error.is_usage());
}

/// The service refuses an expired cursor so. Only an answer from its
/// start can go on, thus the command line must change.
#[tokio::test]
async fn an_expired_cursor_is_its_own_error() {
    let service = MockService::start(Answer::Refuse(Status::failed_precondition(
        "the cursor has expired; send the request again without a cursor",
    )))
    .await;
    let request = tree(no_options()).page(None, Some("OLD".into()));

    let error = client(&service).tree(request).await.unwrap_err();

    assert_eq!(
        error_chain(&error),
        "the query service cannot continue the answer: the cursor has expired; send the request again without a cursor"
    );
    assert!(error.is_usage());
}

/// A request without a cursor has no cursor to expire: another program
/// answers at the address.
#[tokio::test]
async fn a_failed_precondition_without_a_cursor_is_a_failure_of_the_service() {
    let (error, server) = lookup_error(Answer::Refuse(Status::failed_precondition(
        "the system is not ready",
    )))
    .await;

    assert_eq!(
        error_chain(&error),
        format!(
            "the query service at {server} failed with FailedPrecondition: the system is not ready"
        )
    );
    assert!(!error.is_usage());
}

#[tokio::test]
async fn an_empty_message_says_there_was_no_message() {
    let (error, server) = lookup_error(Answer::Refuse(Status::unimplemented(""))).await;

    assert_eq!(
        error_chain(&error),
        format!("the query service at {server} failed with Unimplemented: no message")
    );
    assert!(!error.is_usage());
}

#[tokio::test]
async fn an_answer_that_stops_early_keeps_its_rows_and_tells_why() {
    let service = MockService::start(Answer::Stream(vec![
        Ok((TreeItem::Row(state()), None)),
        Err(Status::internal("the disk is gone")),
    ]))
    .await;

    let reply = client(&service).tree(tree(no_options())).await.unwrap();

    let page = &reply.page;
    assert!(reply.cut_short);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        page.problems,
        [format!(
            "the answer stopped before its end: the query service at {} failed with Internal: the disk is gone",
            service.server
        )]
    );
}

#[tokio::test]
async fn an_answer_that_stops_before_its_first_message_is_an_error() {
    let (error, _) = lookup_error(Answer::Stream(vec![Err(Status::invalid_argument(
        "limit must be at least 1",
    ))]))
    .await;

    assert!(matches!(error, ClientError::Refused { .. }), "{error:?}");
}

#[tokio::test]
async fn a_service_that_is_not_there_is_reported_with_its_address_and_cause() {
    let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address: ServerAddress = format!("http://{}", free.local_addr().unwrap())
        .parse()
        .unwrap();
    drop(free);

    let error = QueryClient::new(address.clone())
        .unwrap()
        .tree(tree(no_options()))
        .await
        .unwrap_err();

    assert!(
        matches!(error, ClientError::Unavailable { .. }),
        "{error:?}"
    );
    let chain = error_chain(&error);
    let expected = format!("the query service at {address} is unavailable: tcp connect error: ");
    assert!(
        chain.starts_with(&expected) && chain.len() > expected.len(),
        "the cause of the failed connect is kept: {chain}"
    );
    assert!(!error.is_usage());
}

#[tokio::test]
async fn an_https_client_sets_up_tls_for_names_and_ip_addresses() {
    for server in [
        "https://burgess.example.com",
        "https://[::1]:8443",
        "https://10.0.0.5",
    ] {
        let client = QueryClient::new(server.parse().unwrap());
        assert!(client.is_ok(), "{server}: {:?}", client.err());
    }
}
