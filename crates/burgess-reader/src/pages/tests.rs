//! Pages built from found rows: what is kept, how rows read, and what is
//! reported.

use std::num::NonZeroUsize;

use burgess_api::{PayloadType, RowKind};
use burgess_core::flow::{FlowId, Subflow, TreeId};
use burgess_core::storage::{StateEntry, StateRow};
use burgess_core::wire;
use jiff::Timestamp;

use super::*;
use crate::test_support::state_row;
use crate::tree_lookup::FoundTree;

fn tree() -> TreeId {
    TreeId::from([1; 12])
}

fn found(rows: Vec<StateRow>) -> FoundTree {
    FoundTree {
        rows,
        problems: Vec::new(),
    }
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

#[test]
fn a_state_reads_as_the_api_row() {
    let page = found(vec![with_payload(
        wire::PayloadType::Json,
        br#"{"amount":42}"#,
    )])
    .into_page(limit(10));

    let [row] = page.rows.as_slice() else {
        panic!("one row expected: {page:?}");
    };
    assert_eq!(row.flow_id, FlowId::new(tree(), Subflow::from(3)));
    assert_eq!(row.tree, tree());
    assert_eq!(row.subflow, Subflow::from(3));
    assert_eq!(
        row.ts,
        Timestamp::from_millisecond(1_793_000_000_123).unwrap()
    );
    assert_eq!(row.kind, RowKind::State);
    assert_eq!(row.payload_type, Some(PayloadType::Json));
    assert_eq!(row.payload.as_ref().unwrap().get(), r#"{"amount":42}"#);
    assert_eq!(row.child_subflow, None);
    assert!(page.problems.is_empty());
}

#[test]
fn a_split_reads_as_the_api_row_of_its_parent() {
    let page = found(vec![state_row(tree(), 2, 5, Some(9))]).into_page(limit(10));

    let row = &page.rows[0];
    assert_eq!(row.kind, RowKind::Split);
    assert_eq!(row.subflow, Subflow::from(2));
    assert_eq!(row.child_subflow, Some(Subflow::from(9)));
    assert!(row.meta.is_empty());
    assert_eq!((row.payload_type, row.payload.is_none()), (None, true));
}

#[test]
fn keeps_the_first_rows_up_to_the_limit_and_says_it_cut() {
    let page = found(states(5)).into_page(limit(3));

    assert_eq!(subflows_of(&page), [0, 1, 2]);
    assert!(page.truncated);
}

#[test]
fn a_page_with_exactly_the_limit_is_not_truncated() {
    let page = found(states(3)).into_page(limit(3));

    assert_eq!(page.rows.len(), 3);
    assert!(!page.truncated);
}

#[test]
fn a_payload_that_is_not_json_is_left_out_and_reported() {
    let page = found(vec![
        with_payload(wire::PayloadType::Json, b"{not json"),
        with_payload(wire::PayloadType::Json, b"\xff"),
    ])
    .into_page(limit(10));

    assert!(page.rows.iter().all(|row| row.payload.is_none()));
    assert_eq!(page.rows[0].payload_type, Some(PayloadType::Json));
    let [not_json, not_text] = page.problems.as_slice() else {
        panic!("two problems expected: {:?}", page.problems);
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
fn a_payload_without_a_type_is_left_out_and_reported() {
    let page =
        found(vec![with_payload(wire::PayloadType::Unspecified, b"{}")]).into_page(limit(10));

    assert_eq!(page.rows[0].payload_type, None);
    assert!(page.rows[0].payload.is_none());
    assert!(page.problems[0].ends_with("has no payload type"));
}

#[test]
fn rows_past_the_limit_report_no_payload_problems() {
    let mut rows = states(1);
    rows.push(with_payload(wire::PayloadType::Json, b"{not json"));

    let page = found(rows).into_page(limit(1));

    assert!(page.problems.is_empty(), "{:?}", page.problems);
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

fn subflows_of(page: &burgess_api::Page) -> Vec<u32> {
    page.rows.iter().map(|row| u32::from(row.subflow)).collect()
}
