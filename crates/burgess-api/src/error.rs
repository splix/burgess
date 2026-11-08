//! Why a request was refused.

use serde::{Deserialize, Serialize};

/// The body of a response to a request the service refused, such as one
/// with an invalid id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// What is wrong with the request, written to be shown as it is.
    pub error: String,
}
