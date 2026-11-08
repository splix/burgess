//! States in the order of their time, which a ListStates request can ask
//! for.
//!
//! The files are sorted by tree, not by time. Thus an ordered lookup keeps
//! the best rows it has read in a heap of the size of the page, and does
//! not keep all rows it reads.

mod smallest;

use burgess_core::wire::UnixMillis;
use jiff::Timestamp;

pub(crate) use smallest::Smallest;

/// The order of states by their time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsOrder {
    /// Earliest first.
    Ascending,
    /// Latest first.
    Descending,
}

impl TsOrder {
    /// `time` as a key that sorts in this order from smallest to largest,
    /// thus one heap of smallest keys serves both orders. Each heap of an
    /// ordered lookup uses this one key.
    pub(crate) fn key(self, time: UnixMillis) -> i64 {
        // The writer stores times after year 9999 as its last instant, and
        // none before the epoch, thus each one and its negation fit.
        let ms = time.saturating_timestamp().as_millisecond();
        match self {
            TsOrder::Ascending => ms,
            TsOrder::Descending => -ms,
        }
    }

    /// The time of `key`, a key of this order.
    pub(crate) fn time(self, key: i64) -> Timestamp {
        let ms = match self {
            TsOrder::Ascending => key,
            TsOrder::Descending => -key,
        };
        Timestamp::from_millisecond(ms).expect("a key is made from a stored time")
    }
}
