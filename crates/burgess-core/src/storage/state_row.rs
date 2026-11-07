//! A row of the states file, as the writer writes it and readers get it back.

use std::collections::BTreeMap;

use crate::flow::{Subflow, TreeId};
use crate::wire::{PayloadType, UnixMillis};

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

/// Which of the two entries a row holds, as the `kind` column names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// [`StateEntry::State`].
    State,
    /// [`StateEntry::Split`].
    Split,
}

impl StateRow {
    /// The order of the states file. It puts a flow's rows next to each
    /// other in time order, and a tree's flows next to each other, so a tree
    /// is one contiguous range of the file.
    pub fn sort_key(&self) -> (TreeId, Subflow, UnixMillis) {
        (self.tree, self.subflow, self.time)
    }
}

impl StateEntry {
    /// Which entry this is.
    pub fn kind(&self) -> EntryKind {
        match self {
            StateEntry::State { .. } => EntryKind::State,
            StateEntry::Split { .. } => EntryKind::Split,
        }
    }
}

impl EntryKind {
    /// Every kind, so that names can be read back.
    const ALL: [EntryKind; 2] = [EntryKind::State, EntryKind::Split];

    /// The value stored in the `kind` column.
    pub fn name(self) -> &'static str {
        match self {
            EntryKind::State => "state",
            EntryKind::Split => "split",
        }
    }

    /// The kind stored as `name`, or `None` if no kind has that name.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.name() == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_is_listed() {
        // A new kind fails to compile here: list it in `ALL` as well.
        let listed = |kind| match kind {
            EntryKind::State | EntryKind::Split => EntryKind::ALL.contains(&kind),
        };
        assert!(listed(EntryKind::State));
        assert!(listed(EntryKind::Split));
    }

    #[test]
    fn kind_names_read_back() {
        for kind in EntryKind::ALL {
            assert_eq!(EntryKind::from_name(kind.name()), Some(kind));
        }
    }

    #[test]
    fn unknown_kind_name_is_none() {
        assert_eq!(EntryKind::from_name("State"), None);
    }
}
