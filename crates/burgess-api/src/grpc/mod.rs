//! The query API over gRPC, made from `proto/burgess/query/v1/query.proto`.
//!
//! The protobuf service is the one definition of the API. The JSON types at
//! the crate root are its REST form. [`Page::from_responses`](crate::Page::from_responses)
//! makes the REST form from the gRPC messages. Thus the service makes an
//! answer one time for the two transports, and a gRPC client can show rows
//! as REST shows them.

mod row_conversion;
mod tree_responses;

pub use row_conversion::RowConversionError;
pub use tree_responses::{TreeItem, TreeResponse};

include!(concat!(env!("OUT_DIR"), "/burgess.query.v1.rs"));

#[cfg(test)]
mod tests;
