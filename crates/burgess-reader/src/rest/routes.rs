//! The routes and what each one looks up.

use std::sync::Arc;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use burgess_api::Page;
use burgess_core::flow::{FlowId, FlowOrTree};

use super::error::RequestError;
use super::params::PageParams;
use crate::queries::Queries;

/// The query API's routes.
pub fn router(queries: Arc<Queries>) -> Router {
    Router::new()
        .route("/v1/trees/{id}", get(tree))
        .route("/v1/flows/{id}", get(flow))
        .fallback(no_route)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(queries)
}

async fn no_route() -> RequestError {
    RequestError::NoRoute
}

async fn method_not_allowed() -> RequestError {
    RequestError::MethodNotAllowed
}

/// Every row of a tree. A flow id names its tree, so an id copied from any
/// row of the tree works.
async fn tree(
    State(queries): State<Arc<Queries>>,
    id: Result<Path<String>, PathRejection>,
    params: Result<Query<PageParams>, QueryRejection>,
) -> Result<Json<Page>, RequestError> {
    let tree = id?.0.parse::<FlowOrTree>()?.tree();
    let request = params?.0.request(&queries.limits)?;
    Ok(Json(queries.tree(tree, request).await))
}

/// Every row of a flow and of the flows split off below it.
async fn flow(
    State(queries): State<Arc<Queries>>,
    id: Result<Path<String>, PathRejection>,
    params: Result<Query<PageParams>, QueryRejection>,
) -> Result<Json<Page>, RequestError> {
    let flow = id?.0.parse::<FlowId>()?;
    let request = params?.0.request(&queries.limits)?;
    Ok(Json(queries.flow(flow, request).await))
}
