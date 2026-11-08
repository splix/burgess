//! One reference to an outside id, as a lookup returns it.

use burgess_core::flow::{FlowId, Subflow, TreeId};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::ids;

/// A flow that recorded a reference to an outside id, such as an order id.
///
/// One row for each reference, without grouping by tree, thus a client
/// sees each flow that used the id and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefRow {
    /// The kind of the outside id, for example `order_id`.
    pub ref_type: String,
    /// The outside id.
    pub domain_id: String,
    /// The service that recorded the reference.
    pub service: String,
    /// The category that recorded the reference, when it is known.
    pub category: Option<String>,
    /// The flow that recorded the reference: `tree` followed by `subflow`.
    #[serde(with = "ids::text")]
    pub flow_id: FlowId,
    /// The tree of the flow, to get all its rows with one lookup.
    #[serde(with = "ids::text")]
    pub tree: TreeId,
    /// The flow's number within its tree; 0 is the root.
    #[serde(with = "ids::subflow")]
    pub subflow: Subflow,
    /// When the reference was recorded by the producer clock, in unix
    /// milliseconds.
    #[serde(with = "jiff::fmt::serde::timestamp::millisecond::required")]
    pub ts: Timestamp,
}
