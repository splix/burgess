//! The query API over gRPC, made from `proto/burgess/query/v1/query.proto`.
//!
//! The protobuf service is the one definition of the API. The JSON types at
//! the crate root are its REST form. Rows change between the two forms
//! here, thus the service makes an answer one time for the two transports,
//! and a gRPC client can show rows as REST shows them.

mod row_conversion;
mod tree_responses;

pub use row_conversion::RowConversionError;
pub use tree_responses::TreeResponse;

include!(concat!(env!("OUT_DIR"), "/burgess.query.v1.rs"));

#[cfg(test)]
mod tests;
