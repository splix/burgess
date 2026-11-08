//! The routes and what each one looks up.

use std::sync::Arc;

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::{Page, RefRow};
use burgess_core::flow::{FlowId, FlowOrTree};
use serde_json::Value;

use super::error::RequestError;
use super::pairs::{read_options, read_pairs};
use super::params::PageParams;
use super::refs::RefsParams;
use super::states::StatesParams;
use crate::queries::Queries;

/// The query API's routes.
pub fn router(queries: Arc<Queries>) -> Router {
    Router::new()
        .route("/v1/trees/{id}", get(tree))
        .route("/v1/flows/{id}", get(flow))
        .route("/v1/states", get(states_by_query).post(states_by_body))
        .route("/v1/refs", get(refs_by_query).post(refs_by_body))
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

/// The query string of GET can repeat `meta.<key>`, thus it is read as
/// pairs.
type Pairs = Result<Query<Vec<(String, String)>>, QueryRejection>;

/// The states of a service and category, with the options in the query
/// string.
async fn states_by_query(
    State(queries): State<Arc<Queries>>,
    pairs: Pairs,
) -> Result<Json<Page>, RequestError> {
    states(&queries, read_pairs(pairs?.0)?).await
}

/// The states of a service and category, with the options in a JSON body.
/// The body is read as JSON first, and its options as those of GET are.
async fn states_by_body(
    State(queries): State<Arc<Queries>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Page>, RequestError> {
    states(&queries, read_options(body?.0)?).await
}

async fn states(queries: &Queries, params: StatesParams) -> Result<Json<Page>, RequestError> {
    let request = params.request(&queries.limits)?;
    Ok(Json(Page::from_responses(
        queries.list_states(request).await?,
    )))
}

/// The references to an outside id, with the options in the query string.
async fn refs_by_query(
    State(queries): State<Arc<Queries>>,
    pairs: Pairs,
) -> Result<Json<Page<RefRow>>, RequestError> {
    refs(&queries, read_pairs(pairs?.0)?).await
}

/// The references to an outside id, with the options in a JSON body.
async fn refs_by_body(
    State(queries): State<Arc<Queries>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Page<RefRow>>, RequestError> {
    refs(&queries, read_options(body?.0)?).await
}

async fn refs(queries: &Queries, params: RefsParams) -> Result<Json<Page<RefRow>>, RequestError> {
    let request = params.request(&queries.limits)?;
    Ok(Json(Page::from_responses(
        queries.list_refs(request).await?,
    )))
}
