//! The options of a lookup request, checked the same way for each method.

use std::num::NonZeroUsize;

use burgess_api::grpc::{
    GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest, Order,
};
use burgess_core::storage::Day;
use jiff::Timestamp;

use super::error::{RequestError, TimeField};
use super::non_empty;
use crate::ordering::TsOrder;
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
    /// after it. A refs or states lookup also refuses the rows after it.
    pub(super) until: Option<Timestamp>,
    /// The number of rows that the page can hold.
    pub(super) limit: NonZeroUsize,
}

impl PageOptions<'_> {
    /// The options of a method without an order, checked against what the
    /// service serves.
    pub(super) fn check(self, limits: &PageLimits) -> Result<PageRequest, RequestError> {
        if self.order()?.is_some() {
            return Err(RequestError::Order);
        }
        Ok(self.check_ordered(limits)?.0)
    }

    /// The options of a method that can order its rows by time, checked,
    /// and the order asked for, if any.
    pub(super) fn check_ordered(
        self,
        limits: &PageLimits,
    ) -> Result<(PageRequest, Option<TsOrder>), RequestError> {
        let order = self.order()?;
        if non_empty(self.cursor).is_some() {
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
        let page = PageRequest {
            until,
            limit: limits.limit(limit)?,
        };
        Ok((page, order))
    }
}

impl PageOptions<'_> {
    /// The order asked for. Checked first, thus an order is refused for
    /// itself, whatever the other options are.
    fn order(&self) -> Result<Option<TsOrder>, RequestError> {
        // Protobuf sends an enum as its number, thus an order that is newer
        // than this service comes as a number that it does not know.
        match Order::try_from(self.order) {
            Ok(Order::Unspecified) => Ok(None),
            Ok(Order::TsAscending) => Ok(Some(TsOrder::Ascending)),
            Ok(Order::TsDescending) => Ok(Some(TsOrder::Descending)),
            Err(_) => Err(RequestError::UnknownOrder(self.order)),
        }
    }
}

/// The time `ms` of the option `field`, refused when no day can hold it.
pub(super) fn time(field: TimeField, ms: i64) -> Result<Timestamp, RequestError> {
    let time = Timestamp::from_millisecond(ms).map_err(|_| RequestError::TimeMs { field, ms })?;
    on_a_day(field, time)
}

/// `time`, the value of the option `field`, refused when no day can hold
/// it.
pub(super) fn on_a_day(field: TimeField, time: Timestamp) -> Result<Timestamp, RequestError> {
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

page_options_of!(
    GetTreeRequest,
    GetFlowRequest,
    ListRefsRequest,
    ListStatesRequest
);
