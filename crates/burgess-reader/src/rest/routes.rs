//! The routes and what each one looks up.

use std::fmt::Display;
use std::sync::Arc;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use burgess_api::Page;
use burgess_core::flow::{FlowId, FlowOrTree};
use burgess_core::report::error_chain;

use super::error::RequestError;
use super::params::{PageParams, PageRequest};
use crate::pages::PageLimits;
use crate::tree_lookup::{FoundTree, TreeLookup};

/// What the routes answer from.
pub struct Queries {
    /// Finds the rows of trees and flows.
    pub lookup: TreeLookup,
    /// How many rows a page may hold.
    pub limits: PageLimits,
}

/// The query API's routes.
pub fn router(queries: Queries) -> Router {
    Router::new()
        .route("/v1/trees/{id}", get(tree))
        .route("/v1/flows/{id}", get(flow))
        .fallback(no_route)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(Arc::new(queries))
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
    let found = queries.lookup.tree(tree, request.until).await;
    Ok(Json(page(found, request, tree)))
}

/// Every row of a flow and of the flows split off below it.
async fn flow(
    State(queries): State<Arc<Queries>>,
    id: Result<Path<String>, PathRejection>,
    params: Result<Query<PageParams>, QueryRejection>,
) -> Result<Json<Page>, RequestError> {
    let flow = id?.0.parse::<FlowId>()?;
    let request = params?.0.request(&queries.limits)?;
    let found = queries.lookup.flow(flow, request.until).await;
    Ok(Json(page(found, request, flow)))
}

/// The page to answer with. Problems are logged as well, since a file that
/// cannot be read is the operator's to fix, not the caller's.
fn page(found: FoundTree, request: PageRequest, id: impl Display) -> Page {
    for problem in &found.problems {
        tracing::warn!(%id, problem = %error_chain(problem), "lookup could not read everything");
    }
    found.into_page(request.limit)
}
