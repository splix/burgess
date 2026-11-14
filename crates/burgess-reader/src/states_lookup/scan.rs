//! The order of a states lookup, and where it continues.

use crate::cursor::RowPosition;
use crate::ordering::TsOrder;

/// How a states lookup gives its states, and where it continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatesScan {
    /// In the order they are read, after the row `from` if given.
    AsRead {
        /// The last state that a page gave.
        from: Option<RowPosition>,
    },
    /// By time, after the key `from` if given.
    ByTime {
        /// The order of the times.
        order: TsOrder,
        /// The key of the last state that a page gave.
        from: Option<TimeKey>,
    },
}

/// The key of a state in an answer by time: its time, then where it is
/// stored. Each state has its own key, thus the order is the same on each
/// page, also for states of the same time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimeKey {
    /// [`TsOrder::key`] of the time.
    pub time: i64,
    /// Where the state is stored.
    pub row: RowPosition,
}
