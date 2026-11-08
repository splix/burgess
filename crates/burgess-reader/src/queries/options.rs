//! The options of a lookup request, checked the same way for each method.

use std::num::NonZeroUsize;

use burgess_api::grpc::{GetFlowRequest, GetTreeRequest, Order};
use burgess_core::storage::Day;
use jiff::Timestamp;

use super::error::RequestError;
use crate::pages::PageLimits;

/// The options of a lookup request, as given.
#[derive(Debug, Clone, Copy)]
pub(super) struct PageOptions<'a> {
    until_ms: Option<i64>,
    limit: Option<u32>,
    order: i32,
    cursor: Option<&'a str>,
}

/// The options of a lookup request, checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PageRequest {
    /// The last day that the lookup must read.
    pub(super) until: Option<Timestamp>,
    /// The number of rows that the page can hold.
    pub(super) limit: NonZeroUsize,
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
        let until = self.until_ms.map(until).transpose()?;
        // A limit that is too large for usize is also over the maximum.
        let limit = self
            .limit
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(PageRequest {
            until,
            limit: limits.limit(limit)?,
        })
    }
}

fn until(until_ms: i64) -> Result<Timestamp, RequestError> {
    let until =
        Timestamp::from_millisecond(until_ms).map_err(|_| RequestError::UntilMs { until_ms })?;
    Day::utc(until).map_err(|source| RequestError::UntilDay { until, source })?;
    Ok(until)
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
