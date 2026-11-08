//! The options of a lookup request, checked the same way for each method.

use std::num::NonZeroUsize;

use burgess_api::grpc::{GetFlowRequest, GetTreeRequest, ListRefsRequest, Order};
use burgess_core::storage::Day;
use jiff::Timestamp;

use super::error::{RequestError, TimeField};
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
    /// The end of the request. A tree lookup reads the days up to the day
    /// after it. A refs lookup also refuses the rows after it.
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
        let until = self
            .until_ms
            .map(|until_ms| time(TimeField::Until, until_ms))
            .transpose()?;
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

/// The time `ms` of the option `field`, refused when no day can hold it.
pub(super) fn time(field: TimeField, ms: i64) -> Result<Timestamp, RequestError> {
    let time = Timestamp::from_millisecond(ms).map_err(|_| RequestError::TimeMs { field, ms })?;
    Day::utc(time).map_err(|source| RequestError::TimeDay {
        field,
        time,
        source,
    })?;
    Ok(time)
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

page_options_of!(GetTreeRequest, GetFlowRequest, ListRefsRequest);
