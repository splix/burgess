//! A stored row as the API returns it.

use std::collections::BTreeMap;

use burgess_api::{PayloadType, Row, RowKind};
use burgess_core::flow::FlowId;
use burgess_core::storage::{StateEntry, StateRow};
use burgess_core::wire;
use serde_json::value::RawValue;

/// `row` as the API returns it. A payload that cannot be embedded is left
/// out, and why is added to `problems`.
pub(super) fn api_row(row: StateRow, problems: &mut Vec<String>) -> Row {
    let flow_id = FlowId::new(row.tree, row.subflow);
    // The writer stores times past year 9999 as its last instant, so every
    // stored time fits.
    let ts = row.time.saturating_timestamp();
    let (kind, meta, payload_type, payload, child_subflow) = match row.entry {
        StateEntry::State {
            meta,
            payload_type,
            payload,
        } => {
            let (payload_type, payload) = match payload_type {
                wire::PayloadType::Json => (Some(PayloadType::Json), json(payload)),
                wire::PayloadType::Unspecified => (None, Err(PayloadProblem::NoType)),
            };
            let payload = payload.map(Some).unwrap_or_else(|problem| {
                problems.push(format!("the payload of {flow_id} at {ts} {problem}"));
                None
            });
            (RowKind::State, meta, payload_type, payload, None)
        }
        StateEntry::Split { child } => (RowKind::Split, BTreeMap::new(), None, None, Some(child)),
    };
    Row {
        flow_id,
        tree: row.tree,
        subflow: row.subflow,
        ts,
        kind,
        service: row.service,
        category: row.category,
        meta,
        payload_type,
        payload,
        child_subflow,
    }
}

/// Why a payload cannot be embedded in its row.
#[derive(Debug, thiserror::Error)]
enum PayloadProblem {
    /// The writer stores payloads as producers sent them, so bytes that are
    /// not JSON can reach the reader.
    #[error("is not valid JSON: {0}")]
    NotJson(serde_json::Error),
    #[error("is not UTF-8 text")]
    NotText,
    /// Validation keeps such states out, but a file is read as it was
    /// written.
    #[error("has no payload type")]
    NoType,
}

/// The payload as JSON to embed in the row.
fn json(payload: Vec<u8>) -> Result<Box<RawValue>, PayloadProblem> {
    let text = String::from_utf8(payload).map_err(|_| PayloadProblem::NotText)?;
    RawValue::from_string(text).map_err(PayloadProblem::NotJson)
}
