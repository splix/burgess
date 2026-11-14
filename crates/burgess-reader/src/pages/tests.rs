//! Found rows as the messages of an answer: what is kept, how rows read,
//! and what is reported.

use std::num::NonZeroUsize;

use burgess_api::grpc::tree_row::Entry;
use burgess_api::grpc::{GetTreeResponse, PayloadType, TreeItem, TreeResponse, TreeRow};
use burgess_core::flow::{FlowId, Subflow, TreeId};
use burgess_core::storage::{Day, StateEntry, StateRow};
use burgess_core::wire;

use super::*;
use crate::cursor::{Cursor, Cursors, QueryHash, Window};
use crate::manifests::ManifestReadError;
use crate::test_support::{in_one_file, state_row};
use crate::tree_lookup::FoundTree;

fn tree() -> TreeId {
    TreeId::from([1; 12])
}

fn found(rows: Vec<StateRow>) -> FoundTree {
    FoundTree {
        rows: in_one_file(rows),
        ..FoundTree::default()
    }
}

fn query() -> QueryHash {
    QueryHash::of(&burgess_api::grpc::GetTreeRequest::default())
}

fn cursors() -> Cursors {
    Cursors {
        query: query(),
        window: Window::new(None, None).unwrap(),
        by_time: false,
        oldest: None,
    }
}

fn messages(found: FoundTree, rows: usize) -> Vec<GetTreeResponse> {
    found.into_responses(limit(rows), &cursors())
}

/// The row number that the cursor of each message tells, `None` for a
/// message without a cursor.
fn cursor_rows(messages: &[GetTreeResponse]) -> Vec<Option<u64>> {
    messages
        .iter()
        .map(|message| {
            let text = message.cursor.as_deref()?;
            let cursor = Cursor::decode(text, query()).unwrap();
            Some(cursor.position.row().index)
        })
        .collect()
}

fn states(count: u32) -> Vec<StateRow> {
    (0..count)
        .map(|i| state_row(tree(), i, u64::from(i), None))
        .collect()
}

fn limit(rows: usize) -> NonZeroUsize {
    NonZeroUsize::new(rows).unwrap()
}

fn with_payload(payload_type: wire::PayloadType, payload: &[u8]) -> StateRow {
    let mut row = state_row(tree(), 3, 1_793_000_000_123, None);
    let StateEntry::State {
        payload_type: stored_type,
        payload: stored,
        ..
    } = &mut row.entry
    else {
        unreachable!("a state row");
    };
    *stored_type = payload_type;
    *stored = payload.to_vec();
    row
}

fn items(found: FoundTree, rows: usize) -> Vec<TreeItem> {
    messages(found, rows)
        .into_iter()
        .map(|response| response.into_item().expect("an item"))
        .collect()
}

fn rows(items: &[TreeItem]) -> Vec<&TreeRow> {
    items
        .iter()
        .filter_map(|item| match item {
            TreeItem::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

fn problems(items: &[TreeItem]) -> Vec<&str> {
    items
        .iter()
        .filter_map(|item| match item {
            TreeItem::Problem(problem) => Some(problem.message.as_str()),
            _ => None,
        })
        .collect()
}

fn subflows(items: &[TreeItem]) -> Vec<u32> {
    rows(items).iter().map(|row| row.subflow).collect()
}

#[test]
fn a_state_reads_as_the_grpc_row() {
    let items = items(
        found(vec![with_payload(
            wire::PayloadType::Json,
            br#"{"amount":42}"#,
        )]),
        10,
    );

    let [TreeItem::Row(row)] = items.as_slice() else {
        panic!("one row expected: {items:?}");
    };
    assert_eq!(
        row.flow_id,
        FlowId::new(tree(), Subflow::from(3)).to_string()
    );
    assert_eq!(row.tree, tree().to_string());
    assert_eq!((row.subflow, row.ts_ms), (3, 1_793_000_000_123));
    let Some(Entry::State(state)) = &row.entry else {
        panic!("a state entry expected: {row:?}");
    };
    assert_eq!(state.payload_type(), PayloadType::Json);
    assert_eq!(state.payload.as_deref(), Some(r#"{"amount":42}"#));
}

#[test]
fn a_payload_is_sent_without_whitespace_around_it() {
    let items = items(
        found(vec![with_payload(
            wire::PayloadType::Json,
            b"  {\"a\": 1}\n",
        )]),
        10,
    );

    let Some(Entry::State(state)) = &rows(&items)[0].entry else {
        panic!("a state entry expected: {items:?}");
    };
    assert_eq!(state.payload.as_deref(), Some(r#"{"a": 1}"#));
}

#[test]
fn a_split_reads_as_the_grpc_row_of_its_parent() {
    let items = items(found(vec![state_row(tree(), 2, 5, Some(9))]), 10);

    let row = rows(&items)[0];
    assert_eq!(row.subflow, 2);
    let Some(Entry::Split(split)) = &row.entry else {
        panic!("a split entry expected: {row:?}");
    };
    assert_eq!(split.child_subflow, 9);
}

/// Each row has the cursor after it, also the last one, since rows come
/// after the page.
#[test]
fn keeps_the_first_rows_up_to_the_limit_each_with_the_cursor_after_it() {
    let messages = messages(found(states(5)), 3);

    assert_eq!(cursor_rows(&messages), [Some(0), Some(1), Some(2)]);
    let items: Vec<TreeItem> = messages.into_iter().filter_map(|m| m.into_item()).collect();
    assert_eq!(subflows(&items), [0, 1, 2]);
}

/// The last row of the answer has no cursor, thus a client knows that it
/// has all rows.
#[test]
fn the_last_row_of_the_answer_has_no_cursor() {
    let messages = messages(found(states(3)), 3);

    assert_eq!(cursor_rows(&messages), [Some(0), Some(1), None]);
}

#[test]
fn a_problem_has_no_cursor() {
    let mut found = found(states(2));
    let day: Day = "20261105".parse().unwrap();
    found.problems.push(
        ManifestReadError::WrongDay {
            day,
            found: day.next().unwrap(),
        }
        .into(),
    );

    let messages = messages(found, 1);

    assert_eq!(cursor_rows(&messages), [None, Some(0)]);
}

#[test]
fn the_problems_of_the_lookup_come_first() {
    let mut found = found(states(1));
    let day: Day = "20261105".parse().unwrap();
    found.problems.push(
        ManifestReadError::WrongDay {
            day,
            found: day.next().unwrap(),
        }
        .into(),
    );

    let items = items(found, 10);

    assert!(
        matches!(items.as_slice(), [TreeItem::Problem(_), TreeItem::Row(_)]),
        "{items:?}"
    );
}

#[test]
fn a_payload_that_is_not_json_is_removed_and_reported_before_its_row() {
    let items = items(
        found(vec![
            with_payload(wire::PayloadType::Json, b"{not json"),
            with_payload(wire::PayloadType::Json, b"\xff"),
        ]),
        10,
    );

    assert!(
        matches!(
            items.as_slice(),
            [
                TreeItem::Problem(_),
                TreeItem::Row(_),
                TreeItem::Problem(_),
                TreeItem::Row(_)
            ]
        ),
        "{items:?}"
    );
    for row in rows(&items) {
        let Some(Entry::State(state)) = &row.entry else {
            panic!("a state entry expected: {row:?}");
        };
        assert_eq!(state.payload_type(), PayloadType::Json);
        assert!(state.payload.is_none());
    }
    let [not_json, not_text] = problems(&items)[..] else {
        panic!("two problems expected: {items:?}");
    };
    let flow = FlowId::new(tree(), Subflow::from(3));
    assert!(
        not_json.starts_with(&format!("the payload of {flow} at ")),
        "{not_json}"
    );
    assert!(not_json.contains("is not valid JSON"), "{not_json}");
    assert!(not_text.ends_with("is not UTF-8 text"), "{not_text}");
}

#[test]
fn a_payload_without_a_type_is_removed_and_reported() {
    let items = items(
        found(vec![with_payload(wire::PayloadType::Unspecified, b"{}")]),
        10,
    );

    let Some(Entry::State(state)) = &rows(&items)[0].entry else {
        panic!("a state entry expected: {items:?}");
    };
    assert_eq!(state.payload_type(), PayloadType::Unspecified);
    assert!(state.payload.is_none());
    assert!(problems(&items)[0].ends_with("has no payload type"));
}

#[test]
fn rows_past_the_limit_report_no_payload_problems() {
    let mut rows = states(1);
    rows.push(with_payload(wire::PayloadType::Json, b"{not json"));

    let items = items(found(rows), 1);

    assert!(problems(&items).is_empty(), "{items:?}");
}

#[test]
fn a_request_without_a_limit_gets_the_default() {
    let limits = PageLimits::default();
    assert_eq!(limits.limit(None), Ok(limits.default_limit));
}

#[test]
fn a_limit_up_to_the_maximum_is_served() {
    let limits = PageLimits::default();
    assert_eq!(limits.limit(Some(10_000)), Ok(limit(10_000)));
    assert_eq!(limits.limit(Some(1)), Ok(limit(1)));
}

#[test]
fn a_limit_over_the_maximum_is_refused() {
    let limits = PageLimits::default();
    assert_eq!(
        limits.limit(Some(10_001)),
        Err(LimitError::OverMax {
            requested: 10_001,
            max: limit(10_000)
        })
    );
}

#[test]
fn a_zero_limit_is_refused() {
    assert_eq!(PageLimits::default().limit(Some(0)), Err(LimitError::Zero));
}

#[test]
fn a_default_over_the_maximum_is_refused() {
    let limits = PageLimits {
        default_limit: limit(20),
        max_limit: limit(10),
    };
    assert!(limits.check().is_err());
    assert!(PageLimits::default().check().is_ok());
}
