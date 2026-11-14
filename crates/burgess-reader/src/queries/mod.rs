//! The lookups of the API, made for the gRPC request and response types.
//!
//! The two transports answer through here. gRPC sends the messages as they
//! are. REST reads its request into the same request message and makes its
//! JSON page from the same messages. Thus the two transports give the same
//! rows, limits, problems and errors.

mod continued;
mod error;
mod options;
mod paged;
mod refs;
mod states;

pub use error::{RequestError, TimeField};

use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{
    GetFlowRequest, GetFlowResponse, GetTreeRequest, GetTreeResponse, TreeResponse,
};
use burgess_core::flow::{FlowId, TreeId};
use burgess_core::report::error_chain;

use self::options::{PageOptions, PageRequest};
use crate::cursor::{Cursors, QueryHash, Window};
use crate::manifests::ManifestCache;
use crate::pages::PageLimits;
use crate::refs_lookup::RefsLookup;
use crate::states_lookup::StatesLookup;
use crate::tree_lookup::{FoundTree, TreeLookup};

/// The data that the API answers from.
pub struct Queries {
    /// Finds the rows of trees and flows.
    pub lookup: TreeLookup,
    /// Finds the references to outside ids.
    pub refs: RefsLookup,
    /// Finds the states of a service and category.
    pub states: StatesLookup,
    /// The manifests that tell whether the rows of a cursor are still
    /// stored.
    pub manifests: Arc<ManifestCache>,
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
        let continuation = self
            .continuation(QueryHash::of(&request), request.cursor.as_deref(), || {
                Ok(Window::new(None, page.until)?)
            })
            .await?;
        // One row more than the page holds tells that the page is cut.
        let wanted = page.limit.saturating_add(1);
        let found = self
            .lookup
            .tree(
                tree,
                continuation.window.until(),
                continuation.unordered()?,
                Some(wanted),
            )
            .await;
        // The walk goes oldest day first, thus the rest of the answer
        // starts on the day of the last row given.
        let cursors = self.cursors(&continuation, None, false)?;
        Ok(answer(found, page, &cursors, tree))
    }

    /// The messages of the answer to a GetFlow request.
    pub async fn get_flow(
        &self,
        request: GetFlowRequest,
    ) -> Result<Vec<GetFlowResponse>, RequestError> {
        let flow = request.flow_id.parse::<FlowId>()?;
        let page = PageOptions::from(&request).check(&self.limits)?;
        let continuation = self
            .continuation(QueryHash::of(&request), request.cursor.as_deref(), || {
                Ok(Window::new(None, page.until)?)
            })
            .await?;
        let from = continuation.unordered()?;
        // The flows below the flow are known only from all splits of the
        // tree, thus each page reads the whole tree, and skips the rows of
        // the pages before.
        let mut found = self.lookup.flow(flow, continuation.window.until()).await;
        if let Some(from) = from {
            found.rows.retain(|row| row.position > from);
        }
        let cursors = self.cursors(&continuation, found.oldest, false)?;
        Ok(answer(found, page, &cursors, flow))
    }
}

fn answer<R: TreeResponse>(
    found: FoundTree,
    page: PageRequest,
    cursors: &Cursors,
    id: impl Display,
) -> Vec<R> {
    log_problems(&found.problems, id);
    found.into_responses(page.limit, cursors)
}

/// The log has the problems too: a file that cannot be read is for the
/// operator to repair, not for the caller. `lookup` names what was looked
/// up, without data that a caller gave about a person.
fn log_problems(problems: &[impl Error + 'static], lookup: impl Display) {
    for problem in problems {
        tracing::warn!(%lookup, problem = %error_chain(problem), "lookup could not read everything");
    }
}

#[cfg(test)]
mod tests;

/// `value`, or `None` when it is "". A client that keeps an option in a
/// string sends "" for no value.
fn non_empty<S: AsRef<str>>(value: Option<S>) -> Option<S> {
    value.filter(|value| !value.as_ref().is_empty())
}

/// `value`, or `None` when it is "" or only spaces. For an expression,
/// where only spaces also tell nothing.
fn non_blank<S: AsRef<str>>(value: Option<S>) -> Option<S> {
    value.filter(|value| !value.as_ref().trim().is_empty())
}
