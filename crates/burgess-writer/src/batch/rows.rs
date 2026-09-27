//! Records reshaped into the rows of the two files a batch produces.
//!
//! States and splits go to one file, sorted so a whole flow's history is one
//! contiguous range; references go to another, sorted by the outside id they
//! are looked up by.

use std::collections::BTreeMap;

use burgess_core::flow::{Subflow, TreeId};
use burgess_core::wire::{Body, PayloadType, Record, UnixMillis};

/// A row of the states file: a state, or a split filed under its parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRow {
    /// Tree of the flow the row belongs to.
    pub tree: TreeId,
    /// Subflow the row belongs to; for a split, the parent's.
    pub subflow: Subflow,
    /// When it happened, by the producer clock.
    pub time: UnixMillis,
    /// Service that captured the state or performed the split.
    pub service: String,
    /// Category of the state or of the split.
    pub category: String,
    /// What the row records.
    pub entry: StateEntry,
}

/// What a states-file row records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateEntry {
    /// A snapshot of application state.
    State {
        /// Labels lookups can filter on.
        meta: BTreeMap<String, String>,
        /// How to read the payload.
        payload_type: PayloadType,
        /// The captured state.
        payload: Vec<u8>,
    },
    /// The flow started a child flow.
    Split {
        /// Subflow of the child, in the same tree.
        child: Subflow,
    },
}

/// A row of the references file.
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

impl StateRow {
    /// Sorting by this puts a flow's rows next to each other in time order,
    /// and a tree's flows next to each other.
    pub(super) fn sort_key(&self) -> (TreeId, Subflow, UnixMillis) {
        (self.tree, self.subflow, self.time)
    }
}

impl RefRow {
    /// Sorting by this lets a lookup by outside id read one contiguous range.
    pub(super) fn sort_key(&self) -> (&str, &str) {
        (&self.ref_type, &self.domain_id)
    }
}

/// A record reshaped for the file it goes to.
pub(super) enum Row {
    State(StateRow),
    Ref(RefRow),
}

/// Expects a validated record: the payload type is read as a known value.
impl From<Record> for Row {
    fn from(record: Record) -> Self {
        let tree = record.flow_id.tree();
        let subflow = record.flow_id.subflow();
        let time = record.time;
        match record.body {
            Body::State(state) => Row::State(StateRow {
                tree,
                subflow,
                time,
                entry: StateEntry::State {
                    payload_type: state.payload_type(),
                    meta: state.meta,
                    payload: state.payload,
                },
                service: state.service,
                category: state.category,
            }),
            Body::Split(split) => Row::State(StateRow {
                tree,
                subflow,
                time,
                service: split.service,
                category: split.category,
                entry: StateEntry::Split {
                    child: Subflow::from(split.child_subflow),
                },
            }),
            Body::Reference(reference) => Row::Ref(RefRow {
                ref_type: reference.ref_type,
                domain_id: reference.domain_id,
                service: reference.service,
                category: reference.category,
                tree,
                subflow,
                time,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use burgess_core::flow::FlowId;
    use burgess_core::wire::{Reference, Split, State};

    use super::*;

    const FLOW: &str = "0192f3a1b2c3d4e5f6a7b8c900000005";

    fn row(body: Body) -> Row {
        Row::from(Record {
            flow_id: FLOW.parse().unwrap(),
            time: UnixMillis::from(1_790_000_000_123),
            body,
        })
    }

    fn flow() -> FlowId {
        FLOW.parse().unwrap()
    }

    #[test]
    fn state_keeps_every_field() {
        let meta = BTreeMap::from([("region".to_string(), "eu".to_string())]);
        let Row::State(row) = row(Body::State(State {
            service: "checkout".into(),
            category: "payment".into(),
            meta: meta.clone(),
            payload_type: PayloadType::Json.into(),
            payload: b"{\"a\":1}".to_vec(),
        })) else {
            panic!("expected a state row");
        };
        assert_eq!(
            row,
            StateRow {
                tree: flow().tree(),
                subflow: flow().subflow(),
                time: UnixMillis::from(1_790_000_000_123),
                service: "checkout".into(),
                category: "payment".into(),
                entry: StateEntry::State {
                    meta,
                    payload_type: PayloadType::Json,
                    payload: b"{\"a\":1}".to_vec(),
                },
            }
        );
    }

    #[test]
    fn split_keeps_every_field() {
        let Row::State(row) = row(Body::Split(Split {
            service: "checkout".into(),
            category: "fan-out".into(),
            child_subflow: 9,
        })) else {
            panic!("expected a state row");
        };
        assert_eq!(
            row,
            StateRow {
                tree: flow().tree(),
                subflow: flow().subflow(),
                time: UnixMillis::from(1_790_000_000_123),
                service: "checkout".into(),
                category: "fan-out".into(),
                entry: StateEntry::Split {
                    child: Subflow::from(9)
                },
            }
        );
    }

    #[test]
    fn reference_keeps_every_field() {
        let Row::Ref(row) = row(Body::Reference(Reference {
            ref_type: "order_id".into(),
            domain_id: "ORD-1".into(),
            service: "orders".into(),
            category: Some("lookup".into()),
        })) else {
            panic!("expected a reference row");
        };
        assert_eq!(
            row,
            RefRow {
                ref_type: "order_id".into(),
                domain_id: "ORD-1".into(),
                service: "orders".into(),
                category: Some("lookup".into()),
                tree: flow().tree(),
                subflow: flow().subflow(),
                time: UnixMillis::from(1_790_000_000_123),
            }
        );
    }
}
