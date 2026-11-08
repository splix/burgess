//! Whole command lines against a gRPC query service in the test process:
//! what is printed where, and how the run ends.

use burgess_api::grpc::tree_row::Entry;
use burgess_api::grpc::{Problem, Split, TreeItem, TreeRow};
use burgess_core::report::error_chain;
use clap::Parser;
use tonic::Status;

use super::*;
use crate::test_support::{Answer, MockService};

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
