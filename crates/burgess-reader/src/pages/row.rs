//! A stored row as a gRPC row.

use burgess_api::grpc::tree_row::Entry;
use burgess_api::grpc::{PayloadType, Split, State, TreeRow};
use burgess_core::flow::FlowId;
use burgess_core::storage::{StateEntry, StateRow};
use burgess_core::wire;
use serde_json::value::RawValue;

/// `row` as a gRPC row, and why its payload was removed, if it was.
pub(super) fn tree_row(row: StateRow) -> (TreeRow, Option<String>) {
    let flow_id = FlowId::new(row.tree, row.subflow);
    // The writer stores times after year 9999 as its last instant, thus all
    // stored times fit.
    let ts = row.time.saturating_timestamp();
    let mut problem = None;
    let entry = match row.entry {
        StateEntry::State {
            meta,
            payload_type,
            payload,
        } => {
            let (payload_type, payload) = match payload_type {
                wire::PayloadType::Json => (PayloadType::Json, json(payload)),
                wire::PayloadType::Unspecified => {
                    (PayloadType::Unspecified, Err(PayloadProblem::NoType))
                }
            };
            let payload = match payload {
                Ok(payload) => Some(payload),
                Err(cause) => {
                    problem = Some(format!("the payload of {flow_id} at {ts} {cause}"));
                    None
                }
            };
            Entry::State(State {
                meta,
                payload_type: payload_type as i32,
                payload,
            })
        }
        StateEntry::Split { child } => Entry::Split(Split {
            child_subflow: u32::from(child),
        }),
    };
    let row = TreeRow {
        flow_id: flow_id.to_string(),
        tree: row.tree.to_string(),
        subflow: u32::from(row.subflow),
        ts_ms: ts.as_millisecond(),
        service: row.service,
        category: row.category,
        entry: Some(entry),
    };
    (row, problem)
}

/// Why a payload cannot be in its row.
#[derive(Debug, thiserror::Error)]
enum PayloadProblem {
    /// The writer stores payloads as producers sent them. Thus bytes that
    /// are not JSON can come to the reader.
    #[error("is not valid JSON: {0}")]
    NotJson(serde_json::Error),
    #[error("is not UTF-8 text")]
    NotText,
    /// Validation stops such states, but the reader reads a file as it was
    /// written.
    #[error("has no payload type")]
    NoType,
}

/// The payload as JSON text, without whitespace around the value. A client
/// reads it as JSON, thus it must be valid JSON.
fn json(payload: Vec<u8>) -> Result<String, PayloadProblem> {
    let text = String::from_utf8(payload).map_err(|_| PayloadProblem::NotText)?;
    let value = RawValue::from_string(text).map_err(PayloadProblem::NotJson)?;
    Ok(Box::<str>::from(value).into_string())
}
