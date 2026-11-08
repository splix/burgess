//! Request and response types of the query API.
//!
//! The query service produces these types and the CLI consumes them. With one
//! shared definition, renaming a field on the server breaks the CLI build
//! instead of silently breaking its output.

mod error;
mod ids;
mod page;
mod row;
mod time;

pub use error::ErrorBody;
pub use page::Page;
pub use row::{PayloadType, Row, RowKind};
pub use time::{TimeParseError, parse_time};

#[cfg(test)]
mod tests;
