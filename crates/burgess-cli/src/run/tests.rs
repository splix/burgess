//! Whole command lines against a mock query service: what is printed where,
//! and how the run ends.

use burgess_core::report::error_chain;
use clap::Parser;
use serde_json::json;
use wiremock::matchers::path;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const TREE: &str = "019a5c2e7f00a1b2c3d4e5f6";

async fn serving(body: serde_json::Value, status: u16) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(path(format!("/v1/trees/{TREE}")))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(&server)
        .await;
    server
}

/// Runs `burgess --server <server> <args>`, returning how it ended and what
/// it printed on standard output and standard error.
async fn burgess(
    server: &MockServer,
    args: &[&str],
) -> (Result<Outcome, RunError>, String, String) {
    let uri = server.uri();
    let cli = Cli::try_parse_from(["burgess", "--server", &uri].iter().chain(args)).unwrap();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let outcome = run(cli, &mut out, &mut err).await;
    (
        outcome,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

fn page(problems: &[&str]) -> serde_json::Value {
    json!({"rows": [{
        "flow_id": format!("{TREE}00000000"), "tree": TREE, "subflow": 0,
        "ts": 1_793_880_000_000_u64, "kind": "split", "service": "checkout",
        "category": "payment", "meta": {}, "payload_type": null,
        "payload": null, "child_subflow": 1,
    }], "truncated": false, "problems": problems})
}

#[tokio::test]
async fn a_complete_lookup_prints_its_rows_and_nothing_else() {
    let server = serving(page(&[]), 200).await;

    let (outcome, out, err) = burgess(&server, &["tree", TREE]).await;

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
    let server = serving(page(&["the manifest of 20261105 is invalid"]), 200).await;

    let (outcome, out, err) = burgess(&server, &["-o", "jsonl", "tree", TREE]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert_eq!(out.lines().count(), 1);
    assert!(
        err.starts_with("problem: the manifest of 20261105 is invalid\n"),
        "{err}"
    );
}

#[tokio::test]
async fn a_refusal_is_one_clear_message() {
    let server = serving(json!({"error": "limit must be at least 1"}), 400).await;

    let (outcome, out, _) = burgess(&server, &["tree", TREE]).await;

    let error = outcome.unwrap_err();
    assert_eq!(
        error_chain(&error),
        "the query service answered 400 Bad Request: limit must be at least 1"
    );
    assert!(matches!(&error, RunError::Client(error) if error.is_usage()));
    assert_eq!(out, "", "nothing is printed for a refused lookup");
}

#[tokio::test]
async fn a_server_error_is_one_clear_message() {
    let server = serving(json!({"error": "the service failed"}), 500).await;

    let (outcome, _, _) = burgess(&server, &["tree", TREE]).await;

    let error = outcome.unwrap_err();
    assert_eq!(
        error_chain(&error),
        "the query service answered 500 Internal Server Error: the service failed"
    );
    assert!(matches!(&error, RunError::Client(error) if !error.is_usage()));
}
