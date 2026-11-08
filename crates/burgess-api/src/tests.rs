//! The JSON the API sends, field for field, since clients outside this
//! workspace read it too.

use std::collections::BTreeMap;

use burgess_core::flow::{FlowId, Subflow, TreeId};
use jiff::Timestamp;
use serde_json::json;
use serde_json::value::RawValue;

use super::*;

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
        payload_type: Some(PayloadType::Json),
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

#[test]
fn a_state_embeds_its_payload_as_json() {
    assert_eq!(
        serde_json::to_value(state()).unwrap(),
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
fn a_split_names_its_child_and_has_no_payload() {
    assert_eq!(
        serde_json::to_value(split()).unwrap(),
        json!({
            "flow_id": format!("{TREE}00000000"),
            "tree": TREE,
            "subflow": 0,
            "ts": 1_793_000_000_123_u64,
            "kind": "split",
            "service": "checkout",
            "category": "order",
            "meta": {},
            "payload_type": null,
            "payload": null,
            "child_subflow": 7,
        })
    );
}

#[test]
fn rows_read_back_as_they_were_sent() {
    for row in [state(), split()] {
        let text = serde_json::to_string(&row).unwrap();
        let back: Row = serde_json::from_str(&text).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), text);
    }
}

#[test]
fn a_row_with_an_invalid_id_is_rejected() {
    let mut row = serde_json::to_value(state()).unwrap();
    row["tree"] = json!("not hex");
    assert!(serde_json::from_value::<Row>(row).is_err());
}

#[test]
fn kinds_and_payload_types_added_later_are_read_as_unknown() {
    let mut row = serde_json::to_value(state()).unwrap();
    row["kind"] = json!("note");
    row["payload_type"] = json!("protobuf");

    let row: Row = serde_json::from_value(row).unwrap();

    assert_eq!(row.kind, RowKind::Unknown);
    assert_eq!(row.payload_type, Some(PayloadType::Unknown));
}

#[test]
fn a_row_without_a_child_subflow_has_none() {
    let mut row = serde_json::to_value(state()).unwrap();
    row.as_object_mut().unwrap().remove("child_subflow");

    let row: Row = serde_json::from_value(row).unwrap();

    assert_eq!(row.child_subflow, None);
}

#[test]
fn a_page_without_problems_leaves_them_out() {
    let page: Page = Page {
        rows: Vec::new(),
        truncated: true,
        problems: Vec::new(),
    };
    assert_eq!(
        serde_json::to_value(&page).unwrap(),
        json!({"rows": [], "truncated": true})
    );
    let back: Page = serde_json::from_value(json!({"rows": [], "truncated": false})).unwrap();
    assert!(back.problems.is_empty());
}

#[test]
fn a_page_lists_its_problems() {
    let page: Page = Page {
        rows: Vec::new(),
        truncated: false,
        problems: vec!["the manifest of 20261105 is invalid".into()],
    };
    assert_eq!(
        serde_json::to_value(&page).unwrap()["problems"],
        json!(["the manifest of 20261105 is invalid"])
    );
}

#[test]
fn an_error_is_one_message() {
    let body = ErrorBody {
        error: "bad id".into(),
    };
    assert_eq!(
        serde_json::to_value(body).unwrap(),
        json!({"error": "bad id"})
    );
}
