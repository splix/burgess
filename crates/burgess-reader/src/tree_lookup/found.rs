//! What a tree lookup found.

use burgess_core::storage::StateRow;

use crate::states_lookup::StatesLookupError;

/// The rows a lookup could read, and what it could not read.
///
/// A lookup goes on past a day or a file it cannot read: an investigation
/// wants every row that can still be found, and the problems tell it what
/// may be missing.
#[derive(Debug, Default)]
pub struct FoundTree {
    /// The rows found, day by day, flush by flush in the order the manifest
    /// lists them, and in `(subflow, ts)` order within a file. The order is
    /// stable between calls but not sorted across files.
    pub rows: Vec<StateRow>,
    /// Each manifest or file that could not be read, in walk order.
    pub problems: Vec<StatesLookupError>,
}
