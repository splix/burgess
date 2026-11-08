//! Requests that the gRPC service cannot serve.

use burgess_core::flow::IdParseError;
use burgess_core::report::error_chain;
use tonic::Status;

use crate::queries::PageRequestError;

/// Why the service cannot serve a gRPC request. The service refuses each
/// with `INVALID_ARGUMENT` and a message that tells what to change.
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
    Until {
        /// The value given.
        until_ms: i64,
    },
    /// `until_ms` or `limit` is a value that the service does not serve.
    #[error(transparent)]
    Page(#[from] PageRequestError),
    /// The service does not sort rows yet. It refuses the order and does
    /// not ignore it, thus the caller does not think the rows are sorted.
    #[error("order is not served yet; do not set it to get the rows in the order they are read")]
    Order,
    /// No answer gives a cursor yet.
    #[error("cursors are not served yet")]
    Cursor,
}

impl From<RequestError> for Status {
    fn from(error: RequestError) -> Self {
        Status::invalid_argument(error_chain(&error))
    }
}
