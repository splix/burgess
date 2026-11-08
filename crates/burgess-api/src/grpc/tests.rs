//! gRPC messages read as the REST page: all fields arrive, and what a
//! client cannot read becomes a problem.

use std::collections::BTreeMap;

use burgess_core::flow::{FlowId, Subflow, TreeId};
use serde_json::json;

use super::tree_row::Entry;
use super::*;
use crate::{Page, Row, RowKind};

const TREE: &str = "0102030405060708090a0b0c";

fn tree() -> TreeId {
    TREE.parse().unwrap()
}

fn state() -> TreeRow {
    TreeRow {
        flow_id: FlowId::new(tree(), Subflow::from(7)).to_string(),
        tree: TREE.into(),
        subflow: 7,
        ts_ms: 1_793_000_000_123,
        service: "checkout".into(),
        category: "order".into(),
        entry: Some(Entry::State(State {
            meta: BTreeMap::from([("region".to_string(), "eu".to_string())]),
            payload_type: PayloadType::Json as i32,
            payload: Some(r#"{"amount":42}"#.into()),
        })),
    }
}

fn split() -> TreeRow {
    TreeRow {
        flow_id: FlowId::new(tree(), Subflow::ROOT).to_string(),
        subflow: 0,
        entry: Some(Entry::Split(Split { child_subflow: 7 })),
        ..state()
    }
}

fn row(message: TreeRow) -> Row {
    Row::try_from(message).unwrap()
}

/// The row as REST sends it.
fn json(row: &Row) -> serde_json::Value {
    serde_json::to_value(row).unwrap()
}

#[test]
fn a_state_reads_as_rest_sends_it() {
    assert_eq!(
        json(&row(state())),
        json!({
            "flow_id": format!("{TREE}00000007"),
            "tree": TREE,
            "subflow": 7,
            "ts": 1_793_000_000_123_u64,
            "kind": "state",
            "service": "checkout",
            "category": "order",
            "meta": {"region": "eu"},
            "payload_type": "json",
            "payload": {"amount": 42},
            "child_subflow": null,
        })
    );
}

#[test]
fn a_split_reads_as_rest_sends_it() {
    let row = row(split());

    assert_eq!(row.kind, RowKind::Split);
    assert_eq!(row.child_subflow, Some(Subflow::from(7)));
    assert!(row.meta.is_empty());
    assert_eq!((row.payload_type, row.payload.is_none()), (None, true));
}

#[test]
fn a_state_without_its_payload_reads_without_one() {
    let mut message = state();
    if let Some(Entry::State(state)) = &mut message.entry {
        state.payload = None;
    }

    let row = row(message);

    assert_eq!(row.payload_type, Some(crate::PayloadType::Json));
    assert!(row.payload.is_none());
}

#[test]
fn a_row_of_a_kind_newer_than_the_client_reads_as_unknown() {
    let row = row(TreeRow {
        entry: None,
        ..state()
    });

    assert_eq!(row.kind, RowKind::Unknown);
    assert_eq!(row.service, "checkout");
}

#[test]
fn a_state_without_a_payload_type_reads_as_rest_sends_it() {
    let mut message = state();
    if let Some(Entry::State(state)) = &mut message.entry {
        state.payload_type = PayloadType::Unspecified as i32;
        state.payload = None;
    }

    let row = row(message);

    assert_eq!(json(&row)["payload_type"], serde_json::Value::Null);
}

#[test]
fn a_payload_type_newer_than_the_client_reads_as_unknown() {
    let mut message = state();
    if let Some(Entry::State(state)) = &mut message.entry {
        state.payload_type = 99;
    }

    assert_eq!(row(message).payload_type, Some(crate::PayloadType::Unknown));
}

#[test]
fn a_row_that_a_client_cannot_read_is_refused_with_the_cause() {
    let bad_id = TreeRow {
        flow_id: "zz".into(),
        ..state()
    };
    assert!(matches!(
        Row::try_from(bad_id),
        Err(RowConversionError::Id(_))
    ));

    let other_tree = TreeRow {
        tree: "0c0b0a090807060504030201".into(),
        ..state()
    };
    assert!(matches!(
        Row::try_from(other_tree),
        Err(RowConversionError::IdsDisagree { .. })
    ));

    let other_subflow = TreeRow {
        subflow: 0,
        ..state()
    };
    assert!(matches!(
        Row::try_from(other_subflow),
        Err(RowConversionError::IdsDisagree { .. })
    ));

    let bad_time = TreeRow {
        ts_ms: i64::MAX,
        ..state()
    };
    assert!(matches!(
        Row::try_from(bad_time),
        Err(RowConversionError::Time { ts_ms: i64::MAX })
    ));

    let mut bad_payload = state();
    if let Some(Entry::State(state)) = &mut bad_payload.entry {
        state.payload = Some("{not json".into());
    }
    assert!(matches!(
        Row::try_from(bad_payload),
        Err(RowConversionError::Payload { .. })
    ));
}

fn responses(items: Vec<TreeItem>) -> Vec<GetTreeResponse> {
    items.into_iter().map(GetTreeResponse::from_item).collect()
}

#[test]
fn messages_make_the_page_in_their_order() {
    let page = Page::from_responses(responses(vec![
        TreeItem::Problem(Problem {
            message: "a file is gone".into(),
        }),
        TreeItem::Row(state()),
        TreeItem::Row(split()),
        TreeItem::Truncated,
    ]));

    assert_eq!(page.problems, ["a file is gone"]);
    let subflows: Vec<u32> = page.rows.iter().map(|row| row.subflow.into()).collect();
    assert_eq!(subflows, [7, 0]);
    assert!(page.truncated);
}

#[test]
fn a_page_without_truncated_is_complete() {
    let page = Page::from_responses(responses(vec![TreeItem::Row(state())]));

    assert_eq!(page.rows.len(), 1);
    assert!(!page.truncated);
    assert!(page.problems.is_empty());
}

#[test]
fn what_the_page_cannot_hold_becomes_a_problem_and_the_rest_is_kept() {
    let bad_time = TreeRow {
        ts_ms: i64::MAX,
        ..state()
    };
    let unknown = GetTreeResponse { item: None };

    let mut messages = responses(vec![TreeItem::Row(bad_time), TreeItem::Row(split())]);
    messages.push(unknown);
    let page = Page::from_responses(messages);

    assert_eq!(page.rows.len(), 1);
    let [time, kind] = page.problems.as_slice() else {
        panic!("two problems expected: {:?}", page.problems);
    };
    assert!(time.contains("is out of range"), "{time}");
    assert_eq!(kind, "a message of an unknown kind was skipped");
}

#[test]
fn tree_and_flow_messages_hold_the_same_items() {
    let items = vec![
        TreeItem::Row(state()),
        TreeItem::Problem(Problem {
            message: "a file is gone".into(),
        }),
        TreeItem::Truncated,
    ];

    for item in items {
        let tree = GetTreeResponse::from_item(item.clone()).into_item();
        let flow = GetFlowResponse::from_item(item.clone()).into_item();
        assert_eq!((tree.as_ref(), flow.as_ref()), (Some(&item), Some(&item)));
    }
}
