//! The rows of protobuf in the JSON form.

use burgess_core::flow::{FlowId, IdParseError, Subflow, TreeId};
use jiff::Timestamp;
use serde_json::value::RawValue;

use super::tree_row::Entry;
use super::{PayloadType, StateRow, TreeRow};
use crate::{RefRow, Row, RowKind};

/// Why a protobuf row cannot change to its JSON form, a [`Row`] or a
/// [`RefRow`]. The service sends only rows that change correctly. Other versions of the service, or a
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

/// The flow and the time of a row, checked.
struct Place {
    flow_id: FlowId,
    tree: TreeId,
    subflow: Subflow,
    ts: Timestamp,
}

impl Place {
    fn new(
        flow_id: &str,
        tree: &str,
        subflow: u32,
        ts_ms: i64,
    ) -> Result<Self, RowConversionError> {
        let flow_id: FlowId = flow_id.parse()?;
        let tree: TreeId = tree.parse()?;
        let subflow = Subflow::from(subflow);
        if flow_id != FlowId::new(tree, subflow) {
            return Err(RowConversionError::IdsDisagree {
                flow_id,
                tree,
                subflow,
            });
        }
        let ts =
            Timestamp::from_millisecond(ts_ms).map_err(|_| RowConversionError::Time { ts_ms })?;
        Ok(Place {
            flow_id,
            tree,
            subflow,
            ts,
        })
    }
}

impl TryFrom<TreeRow> for Row {
    type Error = RowConversionError;

    fn try_from(row: TreeRow) -> Result<Self, Self::Error> {
        let Place {
            flow_id,
            tree,
            subflow,
            ts,
        } = Place::new(&row.flow_id, &row.tree, row.subflow, row.ts_ms)?;
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
                converted.payload = json(state.payload, flow_id)?;
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

/// A state of ListStates is a row of the state kind, as a tree lookup
/// gives it, thus a client shows states of both in the same way.
impl TryFrom<StateRow> for Row {
    type Error = RowConversionError;

    fn try_from(row: StateRow) -> Result<Self, Self::Error> {
        let Place {
            flow_id,
            tree,
            subflow,
            ts,
        } = Place::new(&row.flow_id, &row.tree, row.subflow, row.ts_ms)?;
        Ok(Row {
            flow_id,
            tree,
            subflow,
            ts,
            kind: RowKind::State,
            service: row.service,
            category: row.category,
            meta: row.meta,
            payload_type: api_payload_type(row.payload_type),
            payload: json(row.payload, flow_id)?,
            child_subflow: None,
        })
    }
}

impl TryFrom<super::RefRow> for RefRow {
    type Error = RowConversionError;

    fn try_from(row: super::RefRow) -> Result<Self, Self::Error> {
        let Place {
            flow_id,
            tree,
            subflow,
            ts,
        } = Place::new(&row.flow_id, &row.tree, row.subflow, row.ts_ms)?;
        Ok(RefRow {
            ref_type: row.ref_type,
            domain_id: row.domain_id,
            service: row.service,
            category: row.category,
            flow_id,
            tree,
            subflow,
            ts,
        })
    }
}

/// The payload as the JSON it is.
fn json(
    payload: Option<String>,
    flow_id: FlowId,
) -> Result<Option<Box<RawValue>>, RowConversionError> {
    payload
        .map(RawValue::from_string)
        .transpose()
        .map_err(|source| RowConversionError::Payload { flow_id, source })
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
