//! Whole command lines against a gRPC query service in the test process:
//! what is printed where, and how the run ends.

use burgess_api::grpc::tree_row::Entry;
use burgess_api::grpc::{Problem, Split, TreeItem, TreeRow};
use burgess_core::report::error_chain;
use clap::Parser;
use tonic::Status;

use std::num::NonZeroUsize;

use super::*;
use crate::test_support::{Answer, MockService, Received};

const TREE: &str = "019a5c2e7f00a1b2c3d4e5f6";

/// Runs `burgess --server <service> <args>`. Returns how it ended and what
/// it printed on standard output and standard error.
async fn burgess(
    service: &MockService,
    args: &[&str],
) -> (Result<Outcome, RunError>, String, String) {
    let server = service.server.to_string();
    let cli = Cli::try_parse_from(["burgess", "--server", &server].iter().chain(args)).unwrap();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let outcome = run(cli, &mut out, &mut err).await;
    (
        outcome,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

fn split() -> TreeItem {
    TreeItem::Row(TreeRow {
        flow_id: format!("{TREE}00000000"),
        tree: TREE.into(),
        subflow: 0,
        ts_ms: 1_793_880_000_000,
        service: "checkout".into(),
        category: "payment".into(),
        entry: Some(Entry::Split(Split { child_subflow: 1 })),
    })
}

fn problem(message: &str) -> TreeItem {
    TreeItem::Problem(Problem {
        message: message.into(),
    })
}

#[tokio::test]
async fn a_complete_lookup_prints_its_rows_and_nothing_else() {
    let service = MockService::start(Answer::items(vec![split()])).await;

    let (outcome, out, err) = burgess(&service, &["tree", TREE]).await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    assert_eq!(
        out,
        format!(
            "tree {TREE}\n\n\
             2026-11-05T12:00:00.000Z  00000000  checkout/payment  split  -> 00000001\n\n\
             1 row\n"
        )
    );
    assert_eq!(err, "");
}

#[tokio::test]
async fn a_lookup_with_problems_prints_its_rows_and_ends_partial() {
    let service = MockService::start(Answer::items(vec![
        problem("the manifest of 20261105 is invalid"),
        split(),
    ]))
    .await;

    let (outcome, out, err) = burgess(&service, &["-o", "jsonl", "tree", TREE]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert_eq!(out.lines().count(), 1);
    assert!(
        err.starts_with("problem: the manifest of 20261105 is invalid\n"),
        "{err}"
    );
}

#[tokio::test]
async fn a_refusal_is_one_clear_message() {
    let service = MockService::start(Answer::Refuse(Status::invalid_argument(
        "limit must be at least 1",
    )))
    .await;

    let (outcome, out, _) = burgess(&service, &["tree", TREE]).await;

    let error = outcome.unwrap_err();
    assert_eq!(
        error_chain(&error),
        "the query service refused the request: limit must be at least 1"
    );
    assert!(matches!(&error, RunError::Client(error) if error.is_usage()));
    assert_eq!(out, "", "nothing is printed for a refused lookup");
}

#[tokio::test]
async fn a_server_error_is_one_clear_message() {
    let service = MockService::start(Answer::Refuse(Status::internal("the service failed"))).await;

    let (outcome, _, _) = burgess(&service, &["tree", TREE]).await;

    let error = outcome.unwrap_err();
    assert_eq!(
        error_chain(&error),
        format!(
            "the query service at {} failed with Internal: the service failed",
            service.server
        )
    );
    assert!(matches!(&error, RunError::Client(error) if !error.is_usage()));
}

fn state_message(subflow: u32, payload: &str) -> burgess_api::grpc::ListStatesResponse {
    use burgess_api::grpc::{PayloadType, StateRow, list_states_response};
    burgess_api::grpc::ListStatesResponse {
        item: Some(list_states_response::Item::Row(StateRow {
            flow_id: format!("{TREE}{subflow:08x}"),
            tree: TREE.into(),
            subflow,
            ts_ms: 1_793_880_000_000 + i64::from(subflow),
            service: "checkout".into(),
            category: "payment".into(),
            meta: [("region".to_string(), "eu".to_string())].into(),
            payload_type: PayloadType::Json as i32,
            payload: Some(payload.into()),
        })),
        cursor: None,
    }
}

#[tokio::test]
async fn states_send_each_option_and_print_the_full_flow_ids() {
    use burgess_api::grpc::{ListStatesRequest, Order};
    let service = MockService::start(Answer::States(vec![
        state_message(1, r#"{"amount": 150}"#),
        state_message(2, r#"{"amount": 200}"#),
    ]))
    .await;

    let (outcome, out, err) = burgess(
        &service,
        &[
            "states",
            "checkout",
            "payment",
            "--where",
            "amount > 100",
            "--meta",
            "region=eu",
            "--meta",
            "tier=a=b",
            "--order",
            "desc",
            "--since",
            "1793880000000",
            "--until",
            "2026-11-05T13:00:00Z",
            "--limit",
            "2",
        ],
    )
    .await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    assert_eq!(
        service.received(),
        [Received::States(ListStatesRequest {
            service: "checkout".into(),
            category: "payment".into(),
            r#where: Some("amount > 100".into()),
            meta: [
                ("region".to_string(), "eu".to_string()),
                ("tier".to_string(), "a=b".to_string()),
            ]
            .into(),
            since_ms: Some(1_793_880_000_000),
            until_ms: Some(1_793_883_600_000),
            limit: Some(2),
            order: Order::TsDescending as i32,
            cursor: None,
        })]
    );
    assert_eq!(
        out,
        format!(
            "states of checkout/payment\n\n\
             2026-11-05T12:00:00.001Z  {TREE}00000001  region=eu {{\"amount\":150}}\n\
             2026-11-05T12:00:00.002Z  {TREE}00000002  region=eu {{\"amount\":200}}\n\n\
             2 rows\n"
        )
    );
    assert_eq!(err, "");
}

#[tokio::test]
async fn a_label_given_with_two_values_is_refused_before_any_request() {
    let service = MockService::start(Answer::States(Vec::new())).await;

    let (outcome, _, _) = burgess(
        &service,
        &["states", "a", "b", "--meta", "k=1", "--meta", "k=2"],
    )
    .await;

    let error = outcome.unwrap_err();
    assert_eq!(error.to_string(), "--meta k is given with two values");
    assert!(error.is_usage());
    assert!(service.received().is_empty());
}

#[tokio::test]
async fn a_label_given_two_times_with_one_value_is_sent_one_time() {
    let service = MockService::start(Answer::States(Vec::new())).await;

    let (outcome, _, _) = burgess(
        &service,
        &["states", "a", "b", "--meta", "k=1", "--meta", " k = 1 "],
    )
    .await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    let received = service.received();
    let [Received::States(request)] = received.as_slice() else {
        panic!("one states request");
    };
    assert_eq!(request.meta, [("k".to_string(), "1".to_string())].into());
}

/// A refusal of the filter, as the service sends it, with the column in
/// the metadata when `column` is given.
fn filter_refusal(column: Option<&str>) -> Status {
    let mut status = Status::invalid_argument(
        "invalid where: expected a string, a number, `true`, `false` or `null` at column 10",
    );
    if let Some(column) = column {
        status
            .metadata_mut()
            .insert(burgess_api::grpc::WHERE_COLUMN, column.parse().unwrap());
    }
    status
}

#[tokio::test]
async fn a_refused_filter_is_shown_with_the_expression_and_where_it_fails() {
    let service = MockService::start(Answer::Refuse(filter_refusal(Some("10")))).await;

    let (outcome, out, _) =
        burgess(&service, &["states", "a", "b", "--where", "é\tamount >"]).await;

    let error = outcome.unwrap_err();
    assert_eq!(
        error_chain(&error),
        "the filter \"é\\tamount >\" cannot be read: the query service refused the request: \
         invalid where: expected a string, a number, `true`, `false` or `null` at column 10"
    );
    // Column 10 is the `>`: one cell for `é`, the tab, and seven for
    // `amount `.
    assert_eq!(error.pointer().unwrap(), "  é\tamount >\n   \t       ^");
    assert!(error.is_usage());
    assert_eq!(out, "");
}

#[tokio::test]
async fn a_refusal_without_a_column_is_shown_as_the_service_says_it() {
    let service = MockService::start(Answer::Refuse(filter_refusal(None))).await;

    let (outcome, _, _) = burgess(&service, &["states", "a", "b", "--where", "amount >"]).await;

    let error = outcome.unwrap_err();
    assert!(matches!(error, RunError::Client(_)), "{error:?}");
    assert_eq!(error.pointer(), None);
    assert!(error.is_usage());
}

#[tokio::test]
async fn references_send_their_filters_and_print_who_recorded_each() {
    use burgess_api::grpc::{ListRefsRequest, ListRefsResponse, RefRow, list_refs_response};
    let reference = |subflow: u32, category: Option<&str>| ListRefsResponse {
        item: Some(list_refs_response::Item::Row(RefRow {
            ref_type: "order_id".into(),
            domain_id: "ORD 1".into(),
            service: "checkout".into(),
            category: category.map(str::to_string),
            flow_id: format!("{TREE}{subflow:08x}"),
            tree: TREE.into(),
            subflow,
            ts_ms: 1_793_880_000_000,
        })),
        cursor: None,
    };
    let service = MockService::start(Answer::Refs(vec![
        reference(1, Some("payment")),
        reference(2, None),
    ]))
    .await;

    let (outcome, out, _) = burgess(
        &service,
        &[
            "refs",
            "order_id",
            "ORD 1",
            "--service",
            "checkout",
            "--limit",
            "5",
        ],
    )
    .await;
    let (_, lines, _) = burgess(&service, &["-o", "jsonl", "refs", "order_id", "ORD 1"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    assert_eq!(
        service.received()[0],
        Received::Refs(ListRefsRequest {
            ref_type: "order_id".into(),
            domain_id: "ORD 1".into(),
            service: Some("checkout".into()),
            limit: Some(5),
            ..Default::default()
        })
    );
    assert_eq!(
        out,
        format!(
            "references to order_id \"ORD 1\"\n\n\
             2026-11-05T12:00:00.000Z  {TREE}00000001  checkout/payment\n\
             2026-11-05T12:00:00.000Z  {TREE}00000002  checkout\n\n\
             2 rows\n"
        )
    );
    let first: serde_json::Value = serde_json::from_str(lines.lines().next().unwrap()).unwrap();
    assert_eq!(first["domain_id"], "ORD 1");
    assert_eq!(first["category"], "payment");
}

fn at(column: usize) -> NonZeroUsize {
    NonZeroUsize::new(column).unwrap()
}

#[test]
fn the_mark_is_on_the_line_of_the_error() {
    assert_eq!(pointer("amount\n> x", at(10)), "  > x\n    ^");
    assert_eq!(pointer("a b\r\n= 1", at(3)), "  a b\n    ^");
    assert_eq!(pointer("amount >", at(9)), "  amount >\n          ^");
}

#[test]
fn the_mark_counts_the_cells_of_wide_and_combining_characters() {
    // `x` is the 13th character, after 14 cells: `東京` takes four, and
    // `e` with a combining accent takes one.
    assert_eq!(
        pointer("name = \"東京\" x", at(13)),
        "  name = \"東京\" x\n                ^"
    );
    assert_eq!(pointer("e\u{301} x", at(4)), "  e\u{301} x\n    ^");
}
