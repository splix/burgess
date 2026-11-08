//! Why a request was refused.

use serde::{Deserialize, Serialize};

/// The body of a response to a request the service refused, such as one
/// with an invalid id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// What is wrong with the request, written to be shown as it is.
    pub error: String,
    /// The column of the `where` filter that breaks the grammar, counted in
    /// characters from 1, when that is why. A client can then point to it
    /// without reading the words of `error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_column: Option<usize>,
}
