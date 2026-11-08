//! One stored row, as a lookup returns it.

use std::collections::BTreeMap;

use burgess_core::flow::{FlowId, Subflow, TreeId};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::ids;

/// A state a flow captured, or a split that started a child flow.
///
/// Both kinds share one shape, so a client reads a tree's rows as one list
/// in the order they came. A split is filed under its parent flow, and
/// carries the child in `child_subflow`; it has no meta and no payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// The flow the row belongs to: `tree` followed by `subflow`.
    #[serde(with = "ids::text")]
    pub flow_id: FlowId,
    /// The tree of the flow, so rows of one tree can be grouped without
    /// taking the flow id apart.
    #[serde(with = "ids::text")]
    pub tree: TreeId,
    /// The flow's number within its tree; 0 is the root.
    #[serde(with = "ids::subflow")]
    pub subflow: Subflow,
    /// When it happened by the producer clock, in unix milliseconds.
    #[serde(with = "jiff::fmt::serde::timestamp::millisecond::required")]
    pub ts: Timestamp,
    /// Whether the row is a state or a split.
    pub kind: RowKind,
    /// The service that captured the state or performed the split.
    pub service: String,
    /// The category of the state or of the split.
    pub category: String,
    /// The labels lookups can filter on; empty for a split.
    pub meta: BTreeMap<String, String>,
    /// How the payload is encoded; `None` for a split.
    pub payload_type: Option<PayloadType>,
    /// The state's payload, embedded as the JSON it is, so a client reads
    /// it without decoding a string first. `None` for a split, and for a
    /// stored payload that is not valid JSON, which the page then reports
    /// as a problem.
    pub payload: Option<Box<RawValue>>,
    /// The child flow a split started; `None` for a state.
    #[serde(default, with = "ids::optional_subflow")]
    pub child_subflow: Option<Subflow>,
}

/// Whether a row is a state or a split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RowKind {
    /// A state a flow captured.
    State,
    /// A split that started a child flow.
    Split,
    /// A kind added after this client was built. Read rather than refused,
    /// so one such row does not hide every other row of the page.
    #[serde(other)]
    Unknown,
}

/// How a state's payload is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadType {
    /// JSON, embedded in the row as it is.
    Json,
    /// A type added after this client was built. Read rather than refused,
    /// so one such row does not hide every other row of the page.
    #[serde(other)]
    Unknown,
}
