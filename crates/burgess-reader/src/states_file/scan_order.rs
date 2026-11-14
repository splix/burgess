//! Which states of a file a scan gives first, and where it starts.

use crate::ordering::TsOrder;

/// Which states of a file a scan keeps, and from where.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOrder {
    /// The first states in the order of the file, after the row `after` if
    /// given.
    InFile {
        /// The number of the last row that a page gave.
        after: Option<u64>,
    },
    /// The first states by time, after the key `after` if given.
    ByTime {
        /// The order of the times.
        order: TsOrder,
        /// The key of the last state that a page gave, as this file
        /// compares to it.
        after: Option<FileKey>,
    },
}

/// The key of a state in an order by time: the time as the order keys it,
/// then the row number, thus states of the same time keep their file
/// order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileKey {
    /// [`TsOrder::key`] of the time.
    pub time: i64,
    /// The number of the row in its file.
    pub row: u64,
}
