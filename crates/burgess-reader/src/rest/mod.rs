//! The query API over HTTP, in JSON.
//!
//! A route takes its id from the path and its options from the query
//! string, and answers with a page of rows, or with 400 and an
//! [`ErrorBody`](burgess_api::ErrorBody) when the request cannot be served.
//! A lookup that could not read everything still answers 200: the rows it
//! found are what an investigation needs, and the page lists what may be
//! missing.

mod error;
mod params;
mod routes;

pub use error::RequestError;
pub use routes::{Queries, router};

#[cfg(test)]
mod tests;
