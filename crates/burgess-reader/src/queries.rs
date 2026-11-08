//! The lookups of the API, independent of the transport.
//!
//! REST and gRPC both answer through here. Thus a lookup gives the same
//! rows, limit and problems on the two transports. A transport only reads
//! its request into a [`PageRequest`] and writes the [`Page`].

use std::fmt::Display;
use std::num::NonZeroUsize;

use burgess_api::Page;
use burgess_core::flow::{FlowId, TreeId};
use burgess_core::report::error_chain;
use burgess_core::storage::{Day, DayError};
use jiff::Timestamp;

use crate::pages::{LimitError, PageLimits};
use crate::tree_lookup::{FoundTree, TreeLookup};

/// The data that the API answers from.
pub struct Queries {
    /// Finds the rows of trees and flows.
    pub lookup: TreeLookup,
    /// The number of rows that a page can hold.
    pub limits: PageLimits,
}

/// The options of a lookup request, checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRequest {
    /// The last day that the lookup must read.
    pub until: Option<Timestamp>,
    /// The number of rows that the page can hold.
    pub limit: NonZeroUsize,
}

/// Why the options of a request cannot be served.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PageRequestError {
    /// `until` is on no day that has stored files. Refused, because the
    /// lookup would find nothing and give no cause.
    #[error("until {until} is out of the range of days")]
    Until {
        /// The time given.
        until: Timestamp,
        /// Why it is on no day.
        #[source]
        source: DayError,
    },
    /// The limit is a number that the service does not serve.
    #[error(transparent)]
    Limit(#[from] LimitError),
}

impl PageRequest {
    /// The options of a request, checked against `limits`.
    pub fn new(
        until: Option<Timestamp>,
        limit: Option<usize>,
        limits: &PageLimits,
    ) -> Result<Self, PageRequestError> {
        if let Some(until) = until {
            Day::utc(until).map_err(|source| PageRequestError::Until { until, source })?;
        }
        Ok(PageRequest {
            until,
            limit: limits.limit(limit)?,
        })
    }
}

impl Queries {
    /// The page of all rows of `tree`.
    pub async fn tree(&self, tree: TreeId, request: PageRequest) -> Page {
        let found = self.lookup.tree(tree, request.until).await;
        answer(found, request, tree)
    }

    /// The page of all rows of `flow` and of the flows below it.
    pub async fn flow(&self, flow: FlowId, request: PageRequest) -> Page {
        let found = self.lookup.flow(flow, request.until).await;
        answer(found, request, flow)
    }
}

/// The log has the problems too: a file that cannot be read is for the
/// operator to repair, not for the caller.
fn answer(found: FoundTree, request: PageRequest, id: impl Display) -> Page {
    for problem in &found.problems {
        tracing::warn!(%id, problem = %error_chain(problem), "lookup could not read everything");
    }
    found.into_page(request.limit)
}
