//! The query API over HTTP, in JSON, for curl and simple clients.
//!
//! A route only translates. It reads the path and the query string into the
//! gRPC request message, and makes its JSON page from the gRPC response
//! messages. The lookup is the same as for gRPC. A request that cannot be
//! served gets 400 and an [`ErrorBody`](burgess_api::ErrorBody). A lookup
//! that could not read everything still gets 200: the page has the rows that
//! were found, and the problems tell which rows can be missing.

mod error;
mod params;
mod routes;

pub use error::RequestError;
pub use routes::router;

#[cfg(test)]
mod tests;
