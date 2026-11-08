//! Requests that the service cannot serve.

use std::fmt;

use burgess_core::flow::IdParseError;
use burgess_core::storage::DayError;
use burgess_core::validation::ValidationError;
use jiff::Timestamp;

use crate::filter::FilterError;
use crate::pages::LimitError;
use crate::refs_lookup::UnboundedWalk;

/// Why the service cannot serve a request. Each error is a fault of the
/// request, and its message tells what to change.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// The id is not one that the method accepts.
    #[error("invalid id")]
    Id(#[from] IdParseError),
    /// A GetTree request has no `tree_id` and no `flow_id`.
    #[error("tree_id or flow_id is necessary")]
    NoTree,
    /// A ListStates request has no `service`.
    #[error("service is necessary")]
    NoService,
    /// A ListStates request has no `category`.
    #[error("category is necessary")]
    NoCategory,
    /// The outside id breaks the rules of a reference, thus the writer
    /// never stores it.
    #[error("invalid reference")]
    Reference(#[from] ValidationError),
    /// A time in unix milliseconds is out of the range of times.
    #[error("{field}_ms {ms} is out of the range of times")]
    TimeMs {
        /// The option given.
        field: TimeField,
        /// The value given.
        ms: i64,
    },
    /// A time is on no day that has stored files. Refused, because the
    /// lookup would find nothing and give no cause.
    #[error("{field} {time} is out of the range of days")]
    TimeDay {
        /// The option given.
        field: TimeField,
        /// The time given.
        time: Timestamp,
        /// Why it is on no day.
        #[source]
        source: DayError,
    },
    /// The window ends before it starts, thus no row can be in it.
    #[error("since {since} is after until {until}")]
    SinceAfterUntil {
        /// The start given.
        since: Timestamp,
        /// The end given.
        until: Timestamp,
    },
    /// The walk would have no end.
    #[error(transparent)]
    Walk(#[from] UnboundedWalk),
    /// The limit is a number that the service does not serve.
    #[error(transparent)]
    Limit(#[from] LimitError),
    /// The method does not sort its rows. It refuses the order and does not
    /// ignore it, thus the caller does not think the rows are sorted.
    #[error(
        "only ListStates serves an order; do not set it to get the rows in the order they are read"
    )]
    Order,
    /// The order is not one that this service knows.
    #[error("unknown order {0}")]
    UnknownOrder(i32),
    /// The filter on the payload breaks the grammar. Refused, and not taken
    /// as a filter that no state passes.
    #[error("invalid where")]
    Where(#[from] FilterError),
    /// No answer gives a cursor yet.
    #[error("cursors are not served yet")]
    Cursor,
}

/// A time option of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeField {
    /// The start of the window.
    Since,
    /// The end of the window.
    Until,
}

impl fmt::Display for TimeField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeField::Since => f.write_str("since"),
            TimeField::Until => f.write_str("until"),
        }
    }
}
