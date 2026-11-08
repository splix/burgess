//! The routes and what each one looks up.

use std::sync::Arc;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use burgess_api::Page;
use burgess_api::grpc::get_tree_request::Tree;
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

/// All rows of a tree. A flow id identifies its tree, thus an id from any
/// row of the tree is correct.
async fn tree(
    State(queries): State<Arc<Queries>>,
    id: Result<Path<String>, PathRejection>,
    params: Result<Query<PageParams>, QueryRejection>,
) -> Result<Json<Page>, RequestError> {
    let tree = match id?.0.parse::<FlowOrTree>()? {
        FlowOrTree::Tree(tree) => Tree::TreeId(tree.to_string()),
        FlowOrTree::Flow(flow) => Tree::FlowId(flow.to_string()),
    };
    let request = params?.0.tree_request(tree, &queries.limits)?;
    Ok(Json(Page::from_responses(queries.get_tree(request).await?)))
}

/// All rows of a flow and of the flows split off below it.
async fn flow(
    State(queries): State<Arc<Queries>>,
    id: Result<Path<String>, PathRejection>,
    params: Result<Query<PageParams>, QueryRejection>,
) -> Result<Json<Page>, RequestError> {
    let flow = id?.0.parse::<FlowId>()?;
    let request = params?.0.flow_request(flow, &queries.limits)?;
    Ok(Json(Page::from_responses(queries.get_flow(request).await?)))
}
