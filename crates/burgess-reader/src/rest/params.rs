//! The options that a lookup request takes from its query string.

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{GetFlowRequest, GetTreeRequest};
use burgess_core::flow::FlowId;
use serde::Deserialize;

use super::error::RequestError;
use super::values::{LimitValue, TimeValue, limit, order, time_ms};
use crate::pages::PageLimits;
use crate::queries::TimeField;

/// The query string as given. The values are text, thus a bad value gets
/// the error body of the API, not the error of the query parser.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PageParams {
    until: Option<TimeValue>,
    limit: Option<LimitValue>,
    /// Taken so that the lookup refuses it as for the other routes.
    order: Option<String>,
    cursor: Option<String>,
}

impl PageParams {
    /// The GetTree request for `tree` with these options.
    pub(super) fn tree_request(
        &self,
        tree: Tree,
        limits: &PageLimits,
    ) -> Result<GetTreeRequest, RequestError> {
        Ok(GetTreeRequest {
            tree: Some(tree),
            until_ms: time_ms(TimeField::Until, self.until.as_ref())?,
            limit: limit(self.limit.as_ref(), limits)?,
            order: order(self.order.as_deref())?,
            cursor: self.cursor.clone(),
        })
    }

    /// The GetFlow request for `flow` with these options.
    pub(super) fn flow_request(
        &self,
        flow: FlowId,
        limits: &PageLimits,
    ) -> Result<GetFlowRequest, RequestError> {
        Ok(GetFlowRequest {
            flow_id: flow.to_string(),
            until_ms: time_ms(TimeField::Until, self.until.as_ref())?,
            limit: limit(self.limit.as_ref(), limits)?,
            order: order(self.order.as_deref())?,
            cursor: self.cursor.clone(),
        })
    }
}
