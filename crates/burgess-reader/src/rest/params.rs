//! The options that a lookup request takes from its query string.

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{GetFlowRequest, GetTreeRequest};
use burgess_api::parse_time;
use burgess_core::flow::FlowId;
use serde::Deserialize;

use super::error::RequestError;
use crate::pages::{LimitError, PageLimits};
use crate::queries;

/// The query string as given. The values are text, thus a bad value gets
/// the error body of the API, not the error of the query parser.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PageParams {
    until: Option<String>,
    limit: Option<String>,
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
            until_ms: self.until_ms()?,
            limit: self.limit(limits)?,
            ..Default::default()
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
            until_ms: self.until_ms()?,
            limit: self.limit(limits)?,
            ..Default::default()
        })
    }

    /// REST also accepts RFC 3339, as people write times.
    fn until_ms(&self) -> Result<Option<i64>, RequestError> {
        let until = self.until.as_deref().map(parse_time).transpose()?;
        Ok(until.map(|until| until.as_millisecond()))
    }

    /// gRPC sends the limit as a u32. A larger number is over the maximum,
    /// and REST refuses it with the same words.
    fn limit(&self, limits: &PageLimits) -> Result<Option<u32>, RequestError> {
        let Some(text) = &self.limit else {
            return Ok(None);
        };
        let limit: usize = text.parse().map_err(|_| RequestError::LimitNotANumber {
            found: text.clone(),
        })?;
        let over_max = || {
            queries::RequestError::Limit(LimitError::OverMax {
                requested: limit,
                max: limits.max_limit,
            })
        };
        Ok(Some(u32::try_from(limit).map_err(|_| over_max())?))
    }
}
