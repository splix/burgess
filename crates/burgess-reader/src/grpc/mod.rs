//! The query API over gRPC, the main transport.
//!
//! An answer is a stream: the problems of the lookup, then one row in each
//! message, then a message that tells if the limit cut the answer. A
//! problem about a payload comes just before the row of that payload. A
//! request that the service cannot serve gets `INVALID_ARGUMENT`. A method
//! that is not served yet gets `UNIMPLEMENTED`.
//!
//! The service uses the REST listener. [`router`] adds it as one more
//! route, found by its path. Thus one address serves the two transports,
//! and a stop lets the answers of the two complete.

mod service;

pub use service::{Answer, QueryApi};

use std::sync::Arc;

use axum::Router;
use burgess_api::grpc::query_service_server::QueryServiceServer;
use tonic::server::NamedService;

use crate::queries::Queries;

/// The route of the gRPC service: all methods below the path of the
/// service.
///
/// Answers are not compressed. Tonic compresses each message separately,
/// and with one row in each message that costs more than it saves.
pub fn router(queries: Arc<Queries>) -> Router {
    let path = format!("/{}/{{*method}}", QueryServiceServer::<QueryApi>::NAME);
    Router::new().route_service(&path, QueryServiceServer::new(QueryApi::new(queries)))
}

#[cfg(test)]
mod tests;
