//! The options of a lookup request, checked the same way for each method.

use burgess_api::grpc::{GetFlowRequest, GetTreeRequest, Order};
use jiff::Timestamp;

use super::error::RequestError;
use crate::pages::PageLimits;
use crate::queries::PageRequest;

/// The options of a lookup request, as given.
#[derive(Debug, Clone, Copy)]
pub(super) struct PageOptions<'a> {
    until_ms: Option<i64>,
    limit: Option<u32>,
    order: i32,
    cursor: Option<&'a str>,
}

impl PageOptions<'_> {
    /// The options, checked against what the service serves.
    pub(super) fn check(self, limits: &PageLimits) -> Result<PageRequest, RequestError> {
        // Protobuf sends an enum as its number. Thus an unknown order comes
        // as a number, and the service refuses it the same way.
        if self.order != Order::Unspecified as i32 {
            return Err(RequestError::Order);
        }
        // A client that keeps the cursor in a string sends "" for no cursor.
        if self.cursor.is_some_and(|cursor| !cursor.is_empty()) {
            return Err(RequestError::Cursor);
        }
        let until = self
            .until_ms
            .map(|until_ms| {
                Timestamp::from_millisecond(until_ms).map_err(|_| RequestError::Until { until_ms })
            })
            .transpose()?;
        // A limit that is too large for usize is also over the maximum.
        let limit = self
            .limit
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(PageRequest::new(until, limit, limits)?)
    }
}

/// The requests that have the same options, with the same names.
macro_rules! page_options_of {
    ($($request:ty),+) => {$(
        impl<'a> From<&'a $request> for PageOptions<'a> {
            fn from(request: &'a $request) -> Self {
                PageOptions {
                    until_ms: request.until_ms,
                    limit: request.limit,
                    order: request.order,
                    cursor: request.cursor.as_deref(),
                }
            }
        }
    )+};
}

page_options_of!(GetTreeRequest, GetFlowRequest);
