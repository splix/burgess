//! The options of a ListStates request, as GET and POST take them.

use std::collections::BTreeMap;

use burgess_api::grpc::ListStatesRequest;
use serde::Deserialize;

use super::error::RequestError;
use super::values::{LimitValue, TimeValue, limit, order, time_ms};
use crate::pages::PageLimits;
use crate::queries::TimeField;

/// The options with their REST names. The lookup checks them, as it does
/// for gRPC; this only reads them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StatesParams {
    #[serde(default)]
    service: String,
    #[serde(default)]
    category: String,
    #[serde(rename = "where")]
    filter: Option<String>,
    #[serde(default)]
    meta: BTreeMap<String, String>,
    since: Option<TimeValue>,
    until: Option<TimeValue>,
    order: Option<String>,
    limit: Option<LimitValue>,
    cursor: Option<String>,
}

impl StatesParams {
    /// The ListStates request of these options.
    pub(super) fn request(self, limits: &PageLimits) -> Result<ListStatesRequest, RequestError> {
        Ok(ListStatesRequest {
            since_ms: time_ms(TimeField::Since, self.since.as_ref())?,
            until_ms: time_ms(TimeField::Until, self.until.as_ref())?,
            order: order(self.order.as_deref())?,
            limit: limit(self.limit.as_ref(), limits)?,
            service: self.service,
            category: self.category,
            r#where: self.filter,
            meta: self.meta,
            cursor: self.cursor,
        })
    }
}
