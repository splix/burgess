//! A tree row from protobuf to the JSON form.

use burgess_core::flow::{FlowId, IdParseError, Subflow, TreeId};
use jiff::Timestamp;
use serde_json::value::RawValue;

use super::tree_row::Entry;
use super::{PayloadType, TreeRow};
use crate::{Row, RowKind};

/// Why a protobuf row cannot change to a [`Row`]. The service sends only
/// rows that change correctly. Other versions of the service, or a
/// different program at its address, can send other rows.
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

/// Protobuf sends an enum as its number. A type that is newer than this
/// client comes as a number that it does not know.
fn api_payload_type(number: i32) -> Option<crate::PayloadType> {
    match PayloadType::try_from(number) {
        Ok(PayloadType::Json) => Some(crate::PayloadType::Json),
        Ok(PayloadType::Unspecified) => None,
        Err(_) => Some(crate::PayloadType::Unknown),
    }
}
