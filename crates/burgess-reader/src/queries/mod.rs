//! The lookups of the API, made for the gRPC request and response types.
//!
//! The two transports answer through here. gRPC sends the messages as they
//! are. REST reads its request into the same request message and makes its
//! JSON page from the same messages. Thus the two transports give the same
//! rows, limits, problems and errors.

mod error;
mod options;

pub use error::RequestError;

use std::fmt::Display;

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{
    GetFlowRequest, GetFlowResponse, GetTreeRequest, GetTreeResponse, TreeResponse,
};
use burgess_core::flow::{FlowId, TreeId};
use burgess_core::report::error_chain;

use self::options::{PageOptions, PageRequest};
use crate::pages::PageLimits;
use crate::tree_lookup::{FoundTree, TreeLookup};

/// The data that the API answers from.
pub struct Queries {
    /// Finds the rows of trees and flows.
    pub lookup: TreeLookup,
    /// The number of rows that a page can hold.
    pub limits: PageLimits,
}

impl Queries {
    /// The messages of the answer to a GetTree request.
    pub async fn get_tree(
        &self,
        request: GetTreeRequest,
    ) -> Result<Vec<GetTreeResponse>, RequestError> {
        let tree = match &request.tree {
            Some(Tree::TreeId(tree)) => tree.parse::<TreeId>()?,
            Some(Tree::FlowId(flow)) => flow.parse::<FlowId>()?.tree(),
            None => return Err(RequestError::NoTree),
        };
        let page = PageOptions::from(&request).check(&self.limits)?;
        let found = self.lookup.tree(tree, page.until).await;
        Ok(answer(found, page, tree))
    }

    /// The messages of the answer to a GetFlow request.
    pub async fn get_flow(
        &self,
        request: GetFlowRequest,
    ) -> Result<Vec<GetFlowResponse>, RequestError> {
        let flow = request.flow_id.parse::<FlowId>()?;
        let page = PageOptions::from(&request).check(&self.limits)?;
        let found = self.lookup.flow(flow, page.until).await;
        Ok(answer(found, page, flow))
    }
}

/// The log has the problems too: a file that cannot be read is for the
/// operator to repair, not for the caller.
fn answer<R: TreeResponse>(found: FoundTree, page: PageRequest, id: impl Display) -> Vec<R> {
    for problem in &found.problems {
        tracing::warn!(%id, problem = %error_chain(problem), "lookup could not read everything");
    }
    found.into_responses(page.limit)
}
