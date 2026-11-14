//! What the pages before gave of the files of a flush.

use std::cmp::Ordering;

use super::position::RowPosition;
use crate::manifests::FlushGroup;

/// The order in which a walk reads the flushes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Walk {
    NewestFirst,
    OldestFirst,
}

/// The rows of the files of a flush that the pages before gave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Given {
    /// All rows: the walk read the flush before the last row given.
    All,
    /// The rows up to this row: the last row given is in the flush.
    UpTo(u64),
    /// No row: the walk reads the flush after the last row given.
    Nothing,
}

impl Given {
    /// What the pages that ended at `last` gave of `group`, for a walk in
    /// `walk` order.
    pub(crate) fn of(group: &FlushGroup, last: RowPosition, walk: Walk) -> Given {
        let order = (group.day(), group.nnnn()).cmp(&(last.day, last.nnnn));
        let order = match walk {
            Walk::OldestFirst => order,
            Walk::NewestFirst => order.reverse(),
        };
        match order {
            Ordering::Less => Given::All,
            Ordering::Equal => Given::UpTo(last.index),
            Ordering::Greater => Given::Nothing,
        }
    }

    /// Where a read of the files of `group` starts, for a walk in `walk`
    /// order after the pages that ended at `last`, if given: `None` when
    /// the pages gave all their rows, or else the last row that they gave,
    /// if any.
    pub(crate) fn read_after(
        group: &FlushGroup,
        last: Option<RowPosition>,
        walk: Walk,
    ) -> Option<Option<u64>> {
        match last.map(|last| Given::of(group, last, walk)) {
            Some(Given::All) => None,
            Some(Given::UpTo(index)) => Some(Some(index)),
            Some(Given::Nothing) | None => Some(None),
        }
    }
}
