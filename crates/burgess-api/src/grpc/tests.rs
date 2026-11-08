//! Rows and pages as gRPC messages: all fields arrive, and the conversion
//! refuses what it cannot hold.

use std::collections::BTreeMap;

use burgess_core::flow::{FlowId, Subflow, TreeId};
use jiff::Timestamp;
use serde_json::value::RawValue;

use super::get_flow_response;
use super::get_tree_response::Item;
use super::tree_row::Entry;
use super::*;
use crate::{Page, Row, RowKind};

const TREE: &str = "0102030405060708090a0b0c";

fn tree() -> TreeId {
    TREE.parse().unwrap()
}

fn state() -> Row {
    Row {
        flow_id: FlowId::new(tree(), Subflow::from(7)),
        tree: tree(),
        subflow: Subflow::from(7),
        ts: Timestamp::from_millisecond(1_793_000_000_123).unwrap(),
        kind: RowKind::State,
        service: "checkout".into(),
        category: "order".into(),
        meta: BTreeMap::from([("region".to_string(), "eu".to_string())]),
        payload_type: Some(crate::PayloadType::Json),
        payload: Some(RawValue::from_string(r#"{"amount":42}"#.into()).unwrap()),
        child_subflow: None,
    }
}

fn split() -> Row {
    Row {
        flow_id: FlowId::new(tree(), Subflow::ROOT),
        subflow: Subflow::ROOT,
        kind: RowKind::Split,
        meta: BTreeMap::new(),
        payload_type: None,
        payload: None,
        child_subflow: Some(Subflow::from(7)),
        ..state()
    }
}

/// The row as REST sends it. The two transports must agree on it.
fn json(row: &Row) -> serde_json::Value {
    serde_json::to_value(row).unwrap()
}

fn message(row: Row) -> TreeRow {
    TreeRow::try_from(row).unwrap()
}

fn round_trip(row: Row) -> Row {
    Row::try_from(message(row)).unwrap()
}

#[test]
fn a_state_carries_every_field() {
    let message = message(state());

    assert_eq!(message.flow_id, format!("{TREE}00000007"));
    assert_eq!(message.tree, TREE);
    assert_eq!((message.subflow, message.ts_ms), (7, 1_793_000_000_123));
    assert_eq!(
        (message.service.as_str(), message.category.as_str()),
        ("checkout", "order")
    );
    let Some(Entry::State(state)) = message.entry else {
        panic!("a state entry expected: {:?}", message.entry);
    };
    assert_eq!(state.meta.get("region").map(String::as_str), Some("eu"));
    assert_eq!(state.payload_type(), PayloadType::Json);
    assert_eq!(state.payload.as_deref(), Some(r#"{"amount":42}"#));
}

#[test]
fn a_split_carries_its_child() {
    let message = message(split());

    assert_eq!(
        message.entry,
        Some(Entry::Split(Split { child_subflow: 7 }))
    );
}

#[test]
fn rows_read_back_as_rest_sends_them() {
    for row in [state(), split()] {
        assert_eq!(json(&round_trip(row.clone())), json(&row));
    }
}

#[test]
fn a_state_without_its_payload_reads_back_without_one() {
    let back = round_trip(Row {
        payload: None,
        ..state()
    });

    assert_eq!(back.payload_type, Some(crate::PayloadType::Json));
    assert!(back.payload.is_none());
}

#[test]
fn a_row_of_an_unknown_kind_reads_back_as_unknown() {
    let row = Row {
        kind: RowKind::Unknown,
        ..state()
    };

    let back = round_trip(row);

    assert_eq!(back.kind, RowKind::Unknown);
    assert_eq!(back.service, "checkout");
}

#[test]
fn a_payload_type_newer_than_the_client_reads_as_unknown() {
    let mut message = message(state());
    let Some(Entry::State(state)) = &mut message.entry else {
        unreachable!("a state entry");
    };
    state.payload_type = 99;

    let row = Row::try_from(message).unwrap();

    assert_eq!(row.payload_type, Some(crate::PayloadType::Unknown));
}

#[test]
fn rows_that_protobuf_cannot_hold_are_refused() {
    let no_child = Row {
        child_subflow: None,
        ..split()
    };
    assert!(matches!(
        TreeRow::try_from(no_child),
        Err(RowConversionError::SplitWithoutChild { .. })
    ));

    let unknown_type = Row {
        payload_type: Some(crate::PayloadType::Unknown),
        ..state()
    };
    assert!(matches!(
        TreeRow::try_from(unknown_type),
        Err(RowConversionError::UnknownPayloadType { .. })
    ));
}

#[test]
fn a_message_that_a_client_cannot_read_is_refused_with_the_cause() {
    let bad_id = TreeRow {
        flow_id: "zz".into(),
        ..message(state())
    };
    assert!(matches!(
        Row::try_from(bad_id),
        Err(RowConversionError::Id(_))
    ));

    let other_tree = TreeRow {
        tree: "0c0b0a090807060504030201".into(),
        ..message(state())
    };
    assert!(matches!(
        Row::try_from(other_tree),
        Err(RowConversionError::IdsDisagree { .. })
    ));

    let other_subflow = TreeRow {
        subflow: 0,
        ..message(state())
    };
    assert!(matches!(
        Row::try_from(other_subflow),
        Err(RowConversionError::IdsDisagree { .. })
    ));

    let bad_time = TreeRow {
        ts_ms: i64::MAX,
        ..message(state())
    };
    assert!(matches!(
        Row::try_from(bad_time),
        Err(RowConversionError::Time { ts_ms: i64::MAX })
    ));

    let mut bad_payload = message(state());
    if let Some(Entry::State(state)) = &mut bad_payload.entry {
        state.payload = Some("{not json".into());
    }
    assert!(matches!(
        Row::try_from(bad_payload),
        Err(RowConversionError::Payload { .. })
    ));
}

fn items(page: Page) -> Vec<Item> {
    page.into_responses::<GetTreeResponse>()
        .map(|response| response.item.unwrap())
        .collect()
}

#[test]
fn a_page_is_sent_as_its_problems_then_its_rows_then_whether_it_was_cut() {
    let page = Page {
        rows: vec![state(), split()],
        truncated: true,
        problems: vec!["a file is gone".into()],
    };

    let items = items(page);

    let [
        Item::Problem(problem),
        Item::Row(first),
        Item::Row(second),
        Item::Truncated(_),
    ] = items.as_slice()
    else {
        panic!("problem, two rows, truncated expected: {items:?}");
    };
    assert_eq!(problem.message, "a file is gone");
    assert_eq!((first.subflow, second.subflow), (7, 0));
}

#[test]
fn a_complete_page_ends_with_its_last_row() {
    let page = Page {
        rows: vec![state()],
        truncated: false,
        problems: Vec::new(),
    };

    assert!(matches!(items(page).as_slice(), [Item::Row(_)]));
}

#[test]
fn a_row_that_protobuf_cannot_hold_is_sent_as_a_problem() {
    let page = Page {
        rows: vec![
            Row {
                child_subflow: None,
                ..split()
            },
            state(),
        ],
        truncated: false,
        problems: Vec::new(),
    };

    let items = items(page);

    let [Item::Problem(problem), Item::Row(_)] = items.as_slice() else {
        panic!("a problem, then the other row expected: {items:?}");
    };
    assert!(
        problem.message.contains("has no child subflow"),
        "{problem:?}"
    );
}

#[test]
fn a_flow_answer_has_the_same_messages_as_a_tree_answer() {
    let page = Page {
        rows: vec![state()],
        truncated: true,
        problems: vec!["a file is gone".into()],
    };

    let items: Vec<_> = page
        .into_responses::<GetFlowResponse>()
        .map(|response| response.item.unwrap())
        .collect();

    assert!(
        matches!(
            items.as_slice(),
            [
                get_flow_response::Item::Problem(_),
                get_flow_response::Item::Row(_),
                get_flow_response::Item::Truncated(_),
            ]
        ),
        "{items:?}"
    );
}
