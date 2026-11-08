//! A tree row as JSON and as protobuf.

use burgess_core::flow::{FlowId, IdParseError, Subflow, TreeId};
use jiff::Timestamp;
use serde_json::value::RawValue;

use super::tree_row::Entry;
use super::{PayloadType, Split, State, TreeRow};
use crate::{Row, RowKind};

/// Why a row cannot change to the other form. The service makes only rows
/// that change correctly. Other versions of the service, or a different
/// program at its address, can send other rows.
#[derive(Debug, thiserror::Error)]
pub enum RowConversionError {
    /// The flow id or the tree id is not correct.
    #[error("invalid id in a row")]
    Id(#[from] IdParseError),
    /// `flow_id` is not `tree` followed by `subflow`.
    #[error("the flow id {flow_id} does not agree with tree {tree} and subflow {}", u32::from(*.subflow))]
    IdsDisagree {
        /// The flow id sent.
        flow_id: FlowId,
        /// The tree sent.
        tree: TreeId,
        /// The subflow sent.
        subflow: Subflow,
    },
    /// The time is out of the range of a row time.
    #[error("the time {ts_ms} of a row is out of range")]
    Time {
        /// The time sent, in unix milliseconds.
        ts_ms: i64,
    },
    /// The payload is not JSON, thus the row cannot hold it as JSON.
    #[error("the payload of {flow_id} is not valid JSON")]
    Payload {
        /// The flow of the row.
        flow_id: FlowId,
        /// Why it is not JSON.
        #[source]
        source: serde_json::Error,
    },
    /// A split row has no child. Protobuf has no form for it.
    #[error("the split of {flow_id} has no child subflow")]
    SplitWithoutChild {
        /// The flow of the row.
        flow_id: FlowId,
    },
    /// The payload type is newer than this version. Protobuf sends a type as
    /// its number, and the number is not known.
    #[error("the payload type of {flow_id} is unknown")]
    UnknownPayloadType {
        /// The flow of the row.
        flow_id: FlowId,
    },
}

impl TryFrom<Row> for TreeRow {
    type Error = RowConversionError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let flow_id = row.flow_id;
        let entry = match row.kind {
            RowKind::State => Some(Entry::State(State {
                meta: row.meta,
                payload_type: grpc_payload_type(row.payload_type, flow_id)? as i32,
                payload: row
                    .payload
                    .map(|payload| Box::<str>::from(payload).into_string()),
            })),
            RowKind::Split => {
                let child = row
                    .child_subflow
                    .ok_or(RowConversionError::SplitWithoutChild { flow_id })?;
                Some(Entry::Split(Split {
                    child_subflow: u32::from(child),
                }))
            }
            // An unset entry is how protobuf sends an unknown kind.
            RowKind::Unknown => None,
        };
        Ok(TreeRow {
            flow_id: flow_id.to_string(),
            tree: row.tree.to_string(),
            subflow: u32::from(row.subflow),
            ts_ms: row.ts.as_millisecond(),
            service: row.service,
            category: row.category,
            entry,
        })
    }
}

impl TryFrom<TreeRow> for Row {
    type Error = RowConversionError;

    fn try_from(row: TreeRow) -> Result<Self, Self::Error> {
        let flow_id: FlowId = row.flow_id.parse()?;
        let tree: TreeId = row.tree.parse()?;
        let subflow = Subflow::from(row.subflow);
        if flow_id != FlowId::new(tree, subflow) {
            return Err(RowConversionError::IdsDisagree {
                flow_id,
                tree,
                subflow,
            });
        }
        let ts = Timestamp::from_millisecond(row.ts_ms)
            .map_err(|_| RowConversionError::Time { ts_ms: row.ts_ms })?;
        let mut converted = Row {
            flow_id,
            tree,
            subflow,
            ts,
            kind: RowKind::Unknown,
            service: row.service,
            category: row.category,
            meta: Default::default(),
            payload_type: None,
            payload: None,
            child_subflow: None,
        };
        match row.entry {
            Some(Entry::State(state)) => {
                converted.kind = RowKind::State;
                converted.payload_type = api_payload_type(state.payload_type);
                converted.payload = state
                    .payload
                    .map(RawValue::from_string)
                    .transpose()
                    .map_err(|source| RowConversionError::Payload { flow_id, source })?;
                converted.meta = state.meta;
            }
            Some(Entry::Split(split)) => {
                converted.kind = RowKind::Split;
                converted.child_subflow = Some(Subflow::from(split.child_subflow));
            }
            None => {}
        }
        Ok(converted)
    }
}

fn grpc_payload_type(
    payload_type: Option<crate::PayloadType>,
    flow_id: FlowId,
) -> Result<PayloadType, RowConversionError> {
    match payload_type {
        Some(crate::PayloadType::Json) => Ok(PayloadType::Json),
        None => Ok(PayloadType::Unspecified),
        Some(crate::PayloadType::Unknown) => {
            Err(RowConversionError::UnknownPayloadType { flow_id })
        }
    }
}

/// Protobuf sends an enum as its number. A type that is newer than this
/// client comes as a number that it does not know.
fn api_payload_type(number: i32) -> Option<crate::PayloadType> {
    match PayloadType::try_from(number) {
        Ok(PayloadType::Json) => Some(crate::PayloadType::Json),
        Ok(PayloadType::Unspecified) => None,
        Err(_) => Some(crate::PayloadType::Unknown),
    }
}
