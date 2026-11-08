//! Requests that the service cannot serve.

use burgess_core::flow::IdParseError;
use burgess_core::storage::DayError;
use jiff::Timestamp;

use crate::pages::LimitError;

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
    /// `until_ms` is out of the range of times.
    #[error("until_ms {until_ms} is out of the range of times")]
    UntilMs {
        /// The value given.
        until_ms: i64,
    },
    /// `until` is on no day that has stored files. Refused, because the
    /// lookup would find nothing and give no cause.
    #[error("until {until} is out of the range of days")]
    UntilDay {
        /// The time given.
        until: Timestamp,
        /// Why it is on no day.
        #[source]
        source: DayError,
    },
    /// The limit is a number that the service does not serve.
    #[error(transparent)]
    Limit(#[from] LimitError),
    /// The service does not sort rows yet. It refuses the order and does
    /// not ignore it, thus the caller does not think the rows are sorted.
    #[error("order is not served yet; do not set it to get the rows in the order they are read")]
    Order,
    /// No answer gives a cursor yet.
    #[error("cursors are not served yet")]
    Cursor,
}
