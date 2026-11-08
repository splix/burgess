//! The options of a ListRefs request, as GET and POST take them.

use burgess_api::grpc::ListRefsRequest;
use serde::Deserialize;

use super::error::RequestError;
use super::values::{LimitValue, TimeValue, limit, order, time_ms};
use crate::pages::PageLimits;
use crate::queries::TimeField;

/// The options with their REST names. The outside id is an option and not
/// a part of the path, thus it can hold any character. The lookup checks
/// the options, as it does for gRPC; this only reads them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RefsParams {
    #[serde(default)]
    ref_type: String,
    #[serde(default)]
    domain_id: String,
    service: Option<String>,
    category: Option<String>,
    since: Option<TimeValue>,
    until: Option<TimeValue>,
    order: Option<String>,
    limit: Option<LimitValue>,
    cursor: Option<String>,
}

impl RefsParams {
    /// The ListRefs request of these options.
    pub(super) fn request(self, limits: &PageLimits) -> Result<ListRefsRequest, RequestError> {
        Ok(ListRefsRequest {
            since_ms: time_ms(TimeField::Since, self.since.as_ref())?,
            until_ms: time_ms(TimeField::Until, self.until.as_ref())?,
            order: order(self.order.as_deref())?,
            limit: limit(self.limit.as_ref(), limits)?,
            ref_type: self.ref_type,
            domain_id: self.domain_id,
            service: self.service,
            category: self.category,
            cursor: self.cursor,
        })
    }
}
