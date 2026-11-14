//! Answers in pages: the CLI asks for the next pages with their cursors,
//! and prints the rows as one answer.

use burgess_api::grpc::tree_row::Entry;
use burgess_api::grpc::{
    ListRefsResponse, ListStatesResponse, PayloadType, RefRow, State, StateRow, TreeItem, TreeRow,
    list_refs_response, list_states_response,
};
use tonic::Status;

use super::{Answer, MockService, Outcome, Received, TREE, burgess, problem};

/// A state of the tree, `ms` milliseconds after its first.
fn row(ms: i64) -> TreeItem {
    TreeItem::Row(TreeRow {
        flow_id: format!("{TREE}00000000"),
        tree: TREE.into(),
        subflow: 0,
        ts_ms: 1_793_880_000_000 + ms,
        service: "checkout".into(),
        category: "payment".into(),
        entry: Some(Entry::State(State {
            meta: Default::default(),
            payload_type: PayloadType::Json as i32,
            payload: Some(format!(r#"{{"n":{ms}}}"#)),
        })),
    })
}

/// The answer to the first request, and the answers after each cursor.
fn pages(first: Answer, after: Vec<(&str, Answer)>) -> Answer {
    let after = after
        .into_iter()
        .map(|(cursor, answer)| (Some(cursor.to_string()), answer));
    Answer::Pages(std::iter::once((None, first)).chain(after).collect())
}

/// The cursors and limits of the requests that `service` received.
fn asked(service: &MockService) -> Vec<(Option<String>, Option<u32>)> {
    service
        .received()
        .into_iter()
        .map(|received| match received {
            Received::Tree(request) => (request.cursor, request.limit),
            Received::Flow(request) => (request.cursor, request.limit),
            Received::States(request) => (request.cursor, request.limit),
            Received::Refs(request) => (request.cursor, request.limit),
        })
        .collect()
}

/// The note that tells how to print the rows after the `shown` rows.
fn more_after(shown: usize, cursor: &str) -> String {
    format!(
        "more rows exist after the {shown} shown; to print them, repeat the command with --cursor {cursor}\n"
    )
}

#[tokio::test]
async fn two_pages_come_out_as_one_listing() {
    let paged = MockService::start(pages(
        Answer::cut(vec![row(0), row(1)], "C1"),
        vec![("C1", Answer::items(vec![row(2)]))],
    ))
    .await;
    let whole = MockService::start(Answer::items(vec![row(0), row(1), row(2)])).await;

    let (outcome, out, err) = burgess(&paged, &["tree", TREE, "--all"]).await;
    let (_, one_page, _) = burgess(&whole, &["tree", TREE]).await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    assert_eq!(out, one_page);
    assert_eq!(err, "");
    assert_eq!(
        asked(&paged),
        [(None, None), (Some("C1".into()), None)],
        "the service's default page size"
    );
}

/// Each command is wired on its own.
#[tokio::test]
async fn each_lookup_asks_for_the_next_page_with_the_cursor() {
    let state = |subflow: u32, cursor: Option<&str>| ListStatesResponse {
        item: Some(list_states_response::Item::Row(StateRow {
            flow_id: format!("{TREE}{subflow:08x}"),
            tree: TREE.into(),
            subflow,
            ts_ms: 1_793_880_000_000,
            ..Default::default()
        })),
        cursor: cursor.map(str::to_string),
    };
    let reference = |subflow: u32, cursor: Option<&str>| ListRefsResponse {
        item: Some(list_refs_response::Item::Row(RefRow {
            ref_type: "order_id".into(),
            domain_id: "ORD-1".into(),
            flow_id: format!("{TREE}{subflow:08x}"),
            tree: TREE.into(),
            subflow,
            ts_ms: 1_793_880_000_000,
            ..Default::default()
        })),
        cursor: cursor.map(str::to_string),
    };
    let flow = format!("{TREE}00000000");
    let lookups: [(Vec<&str>, Answer); 4] = [
        (
            vec!["tree", TREE],
            pages(
                Answer::cut(vec![row(0)], "C1"),
                vec![("C1", Answer::items(vec![row(1)]))],
            ),
        ),
        (
            vec!["flow", &flow],
            pages(
                Answer::cut(vec![row(0)], "C1"),
                vec![("C1", Answer::items(vec![row(1)]))],
            ),
        ),
        (
            vec!["states", "checkout", "payment"],
            pages(
                Answer::States(vec![state(1, Some("C1"))]),
                vec![("C1", Answer::States(vec![state(2, None)]))],
            ),
        ),
        (
            vec!["refs", "order_id", "ORD-1"],
            pages(
                Answer::Refs(vec![reference(1, Some("C1"))]),
                vec![("C1", Answer::Refs(vec![reference(2, None)]))],
            ),
        ),
    ];

    for (lookup, answer) in lookups {
        let service = MockService::start(answer).await;
        let args: Vec<&str> = lookup.iter().copied().chain(["--all"]).collect();

        let (outcome, out, _) = burgess(&service, &args).await;

        assert_eq!(outcome.unwrap(), Outcome::Complete, "{lookup:?}");
        assert!(out.ends_with("2 rows\n"), "{lookup:?}: {out}");
        let cursors: Vec<_> = asked(&service)
            .into_iter()
            .map(|(cursor, _)| cursor)
            .collect();
        assert_eq!(cursors, [None, Some("C1".into())], "{lookup:?}");
    }
}

/// Without a number of rows, the CLI asks as it did before it had pages.
#[tokio::test]
async fn without_paging_options_one_page_is_asked_for_and_its_cursor_printed() {
    let service = MockService::start(pages(
        Answer::cut(vec![row(0)], "C1"),
        vec![("C1", Answer::items(vec![row(1)]))],
    ))
    .await;

    let (outcome, out, err) = burgess(&service, &["tree", TREE]).await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    assert!(out.ends_with("1 row\n"), "{out}");
    assert_eq!(err, more_after(1, "C1"));
    assert_eq!(asked(&service), [(None, None)]);
}

#[tokio::test]
async fn a_limit_asks_for_the_rows_still_to_print_in_pages_of_the_page_size() {
    let service = MockService::start(pages(
        Answer::cut(vec![row(0), row(1)], "C1"),
        vec![("C1", Answer::cut(vec![row(2)], "C2"))],
    ))
    .await;

    let (outcome, out, err) = burgess(
        &service,
        &["tree", TREE, "--limit", "3", "--page-size", "2"],
    )
    .await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    assert!(out.ends_with("3 rows\n"), "{out}");
    assert_eq!(err, more_after(3, "C2"));
    assert_eq!(
        asked(&service),
        [(None, Some(2)), (Some("C1".into()), Some(1))]
    );
}

#[tokio::test]
async fn a_limit_without_a_page_size_is_one_request_of_that_many_rows() {
    let service = MockService::start(Answer::cut(vec![row(0), row(1)], "C1")).await;

    let (_, _, err) = burgess(&service, &["tree", TREE, "--limit", "2"]).await;

    assert_eq!(asked(&service), [(None, Some(2))]);
    assert_eq!(err, more_after(2, "C1"));
}

#[tokio::test]
async fn a_cursor_continues_an_earlier_answer() {
    let service = MockService::start(pages(
        Answer::Refuse(Status::internal("not the first page")),
        vec![("C1", Answer::items(vec![row(1)]))],
    ))
    .await;

    let (outcome, out, err) = burgess(&service, &["tree", TREE, "--cursor", "C1"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    assert!(out.ends_with("1 row\n"), "{out}");
    assert_eq!(err, "");
    assert_eq!(asked(&service), [(Some("C1".into()), None)]);
}

/// The service tells a problem of a file that can hide rows of more pages
/// on each of them.
#[tokio::test]
async fn a_problem_that_more_pages_tell_is_printed_one_time() {
    let broken = "reading 20261105/1-states.parquet failed";
    let service = MockService::start(pages(
        Answer::cut(vec![problem(broken), row(0)], "C1"),
        vec![("C1", Answer::items(vec![problem(broken), row(1)]))],
    ))
    .await;

    let (outcome, _, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert_eq!(err.matches(broken).count(), 1, "{err}");
}

#[tokio::test]
async fn an_expired_cursor_is_refused_as_a_command_line_to_change() {
    let service = MockService::start(Answer::Refuse(Status::failed_precondition(
        "the cursor has expired; send the request again without a cursor",
    )))
    .await;

    let (outcome, out, _) = burgess(&service, &["tree", TREE, "--cursor", "OLD"]).await;

    let error = outcome.unwrap_err();
    assert!(error.is_usage(), "{error:?}");
    assert_eq!(out, "");
}

/// The rows of the pages before are printed, and the cursor of the last
/// page continues after them, as the next page may come later.
#[tokio::test]
async fn a_page_that_fails_after_the_first_keeps_the_rows_before_it() {
    let service = MockService::start(pages(
        Answer::cut(vec![row(0)], "C1"),
        vec![("C1", Answer::Refuse(Status::unavailable("restarting")))],
    ))
    .await;

    let (outcome, out, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert!(out.ends_with("1 row\n"), "{out}");
    assert!(err.contains("the answer stopped before its end"), "{err}");
    assert!(err.ends_with(&more_after(1, "C1")), "{err}");
}

/// The rows after an expired cursor are gone, thus no cursor is printed.
#[tokio::test]
async fn a_page_that_expires_after_the_first_ends_the_answer_without_a_cursor() {
    let service = MockService::start(pages(
        Answer::cut(vec![row(0)], "C1"),
        vec![(
            "C1",
            Answer::Refuse(Status::failed_precondition("the cursor has expired")),
        )],
    ))
    .await;

    let (outcome, out, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert!(out.ends_with("1 row\n"), "{out}");
    assert!(err.contains("the cursor has expired"), "{err}");
    assert!(!err.contains("--cursor"), "{err}");
}

/// The cursor of a broken stream continues after the last row that came,
/// and it is printed, not followed: the stream may break again.
#[tokio::test]
async fn a_broken_stream_ends_the_pages_with_its_cursor() {
    let service = MockService::start(pages(
        Answer::Stream(vec![
            Ok((row(0), Some("C1".into()))),
            Err(Status::internal("the disk is gone")),
        ]),
        vec![("C1", Answer::items(vec![row(1)]))],
    ))
    .await;

    let (outcome, _, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert_eq!(asked(&service).len(), 1);
    assert!(err.ends_with(&more_after(1, "C1")), "{err}");
}

/// A stream that breaks before a row of the page has come continues where
/// the page started.
#[tokio::test]
async fn a_page_that_breaks_before_its_first_row_keeps_the_cursor_before_it() {
    let service = MockService::start(pages(
        Answer::cut(vec![row(0)], "C1"),
        vec![(
            "C1",
            Answer::Stream(vec![
                Ok((problem("a file is gone"), None)),
                Err(Status::internal("the disk is gone")),
            ]),
        )],
    ))
    .await;

    let (outcome, _, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert!(err.ends_with(&more_after(1, "C1")), "{err}");
}

/// The cursor moves on also over rows that the CLI cannot print.
#[tokio::test]
async fn a_page_of_rows_that_cannot_be_printed_moves_on() {
    let unreadable = TreeItem::Row(TreeRow {
        ts_ms: i64::MAX,
        ..match row(0) {
            TreeItem::Row(row) => row,
            TreeItem::Problem(_) => unreachable!(),
        }
    });
    let service = MockService::start(pages(
        Answer::cut(vec![unreadable], "C1"),
        vec![("C1", Answer::items(vec![row(1)]))],
    ))
    .await;

    let (outcome, out, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert!(out.ends_with("1 row\n"), "{out}");
    assert!(!err.contains("--cursor"), "{err}");
    assert_eq!(asked(&service).len(), 2);
}

/// A cursor that does not move on would be asked for without end.
#[tokio::test]
async fn a_service_that_sends_back_the_cursor_it_got_ends_the_answer() {
    let service = MockService::start(pages(
        Answer::cut(vec![row(0)], "C1"),
        vec![("C1", Answer::cut(vec![row(1)], "C1"))],
    ))
    .await;

    let (outcome, out, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert!(out.ends_with("2 rows\n"), "{out}");
    assert!(err.contains("the cursor that it was sent"), "{err}");
    assert!(!err.contains("--cursor"), "{err}");
    assert_eq!(asked(&service).len(), 2);
}

/// Two rows can have problems of the same words. Each is told.
#[tokio::test]
async fn problems_of_the_same_words_in_one_page_are_each_printed() {
    let damaged = "the payload of a row is not valid JSON";
    let service = MockService::start(Answer::items(vec![
        problem(damaged),
        row(0),
        problem(damaged),
        row(1),
    ]))
    .await;

    let (_, _, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(err.matches(damaged).count(), 2, "{err}");
}

/// The service refuses the cursor again, thus it is not printed to repeat.
#[tokio::test]
async fn a_later_page_that_is_refused_ends_the_answer_without_a_cursor() {
    let service = MockService::start(pages(
        Answer::cut(vec![row(0)], "C1"),
        vec![(
            "C1",
            Answer::Refuse(Status::invalid_argument("invalid cursor")),
        )],
    ))
    .await;

    let (outcome, _, err) = burgess(&service, &["tree", TREE, "--all"]).await;

    assert_eq!(outcome.unwrap(), Outcome::Partial);
    assert!(err.contains("invalid cursor"), "{err}");
    assert!(!err.contains("--cursor"), "{err}");
}

/// The service refuses a cursor for a request that is not the request of
/// the cursor, thus each page asks for the same rows.
#[tokio::test]
async fn each_page_asks_with_the_options_of_the_first() {
    let state = |subflow: u32, cursor: Option<&str>| ListStatesResponse {
        item: Some(list_states_response::Item::Row(StateRow {
            flow_id: format!("{TREE}{subflow:08x}"),
            tree: TREE.into(),
            subflow,
            ts_ms: 1_793_880_000_000,
            ..Default::default()
        })),
        cursor: cursor.map(str::to_string),
    };
    let service = MockService::start(pages(
        Answer::States(vec![state(1, Some("C1"))]),
        vec![("C1", Answer::States(vec![state(2, None)]))],
    ))
    .await;

    let (outcome, _, _) = burgess(
        &service,
        &[
            "states",
            "checkout",
            "payment",
            "--where",
            "amount > 100",
            "--meta",
            "region=eu",
            "--order",
            "asc",
            "--since",
            "1793880000000",
            "--until",
            "1793883600000",
            "--limit",
            "5",
        ],
    )
    .await;

    assert_eq!(outcome.unwrap(), Outcome::Complete);
    let received = service.received();
    let [Received::States(first), Received::States(second)] = received.as_slice() else {
        panic!("two states requests: {received:?}");
    };
    assert_eq!(second.cursor.as_deref(), Some("C1"));
    assert_eq!(second.limit, Some(4), "the rows still to print");
    let without_page =
        |request: &burgess_api::grpc::ListStatesRequest| burgess_api::grpc::ListStatesRequest {
            limit: None,
            cursor: None,
            ..request.clone()
        };
    assert_eq!(without_page(second), without_page(first));
}
