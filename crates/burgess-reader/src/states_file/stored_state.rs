//! A state as a scan of states files gives it.

use std::collections::BTreeMap;

use burgess_core::flow::{Subflow, TreeId};
use burgess_core::wire::{PayloadType, UnixMillis};

/// A state row of a states file. A scan keeps states only, thus this type
/// has no split, and its callers do not handle one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredState {
    /// Tree of the flow.
    pub tree: TreeId,
    /// Subflow of the flow.
    pub subflow: Subflow,
    /// When it happened, by the producer clock.
    pub time: UnixMillis,
    /// Service that captured the state.
    pub service: String,
    /// Category of the state.
    pub category: String,
    /// Labels of the state.
    pub meta: BTreeMap<String, String>,
    /// How to read the payload.
    pub payload_type: PayloadType,
    /// The captured state.
    pub payload: Vec<u8>,
}
