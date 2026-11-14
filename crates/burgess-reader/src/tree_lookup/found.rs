//! What a tree lookup found.

use burgess_core::storage::{Day, StateRow};

use crate::cursor::Positioned;
use crate::states_lookup::StatesLookupError;

/// The rows a lookup could read, and what it could not read.
///
/// A lookup goes on past a day or a file it cannot read: an investigation
/// wants every row that can still be found, and the problems tell it what
/// may be missing.
#[derive(Debug, Default)]
pub struct FoundTree {
    /// The rows found, day by day, flush by flush in the order of their
    /// flush time, and in `(subflow, ts)` order within a file. Thus their
    /// positions are in order, which a cursor needs. The order is stable
    /// between calls but not sorted by time across files.
    pub rows: Vec<Positioned<StateRow>>,
    /// Each manifest or file that could not be read and can hide a row of
    /// the page, in walk order.
    pub problems: Vec<StatesLookupError>,
    /// The first day of the walk. `None` when no day can hold the tree.
    pub oldest: Option<Day>,
}
