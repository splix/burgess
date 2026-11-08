//! A row of the refs file, as the writer writes it and readers get it back.

use crate::flow::{Subflow, TreeId};
use crate::wire::UnixMillis;

/// A row of the refs file: a flow is linked to an id from outside Burgess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefRow {
    /// Kind of outside id.
    pub ref_type: String,
    /// The outside id.
    pub domain_id: String,
    /// Service that recorded the link.
    pub service: String,
    /// Category that recorded the link, if known.
    pub category: Option<String>,
    /// Tree of the linked flow.
    pub tree: TreeId,
    /// Subflow of the linked flow.
    pub subflow: Subflow,
    /// When the link was recorded, by the producer clock.
    pub time: UnixMillis,
}

impl RefRow {
    /// The file is sorted by this key, thus a lookup by outside id reads one
    /// contiguous range.
    pub fn sort_key(&self) -> (&str, &str) {
        (&self.ref_type, &self.domain_id)
    }
}
