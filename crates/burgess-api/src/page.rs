//! One response's worth of rows.

use serde::{Deserialize, Serialize};

use crate::row::Row;

/// The rows of one response, and what may be missing from them. The rows
/// of trees and of states are [`Row`]s, and those of references are
/// [`RefRow`](crate::RefRow)s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<R = Row> {
    /// The rows, in the order the lookup read them, or in the order asked
    /// for.
    pub rows: Vec<R>,
    /// Whether the lookup found more rows than the request's limit, so some
    /// were left out. There is no way yet to ask for the rest.
    pub truncated: bool,
    /// Why some rows may be missing or incomplete: a day or a file that
    /// could not be read, or a payload that could not be embedded. A lookup
    /// returns what it could still find, so an investigation is not stopped
    /// by one bad file, and these tell what to distrust.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub problems: Vec<String>,
}
