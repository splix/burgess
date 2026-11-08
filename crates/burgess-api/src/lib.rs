//! Request and response types of the query API.
//!
//! The query service produces these types and its clients consume them. With
//! one shared definition, renaming a field on the server breaks the client
//! build instead of silently breaking its output.
//!
//! [`grpc`] holds the API as the protobuf service, its main form. The types
//! at the root are the JSON shapes REST answers with.

pub mod grpc;

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
