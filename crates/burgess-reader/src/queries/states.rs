//! The ListStates lookup.

use std::mem;

use burgess_api::grpc::{ListStatesRequest, ListStatesResponse};
use jiff::{SignedDuration, Timestamp};

use super::error::{RequestError, TimeField};
use super::options::{PageOptions, on_a_day, time};
use super::{Queries, log_problems, non_blank};
use crate::cursor::{Position, QueryHash, Window};
use crate::filter::Filter;
use crate::ordering::TsOrder;
use crate::states_file::StatesFilter;
use crate::states_lookup::{StatesScan, TimeKey};

/// The window of a request without `since`: the day of states before
/// `until`.
const DEFAULT_WINDOW: SignedDuration = SignedDuration::from_hours(24);

impl Queries {
    /// The messages of the answer to a ListStates request.
    ///
    /// A window without an end is made absolute here, one time, thus all
    /// days and files of the lookup use the same window. The cursor keeps
    /// it, thus the next pages use it too, and not a later "now".
    pub async fn list_states(
        &self,
        mut request: ListStatesRequest,
    ) -> Result<Vec<ListStatesResponse>, RequestError> {
        if request.service.is_empty() {
            return Err(RequestError::NoService);
        }
        if request.category.is_empty() {
            return Err(RequestError::NoCategory);
        }
        // Only spaces is no filter. The text is parsed as sent, thus the
        // column of an error counts in it.
        let payload = non_blank(request.r#where.as_deref())
            .map(str::parse::<Filter>)
            .transpose()?;
        let (page, order) = PageOptions::from(&request).check_ordered(&self.limits)?;
        let continuation = self
            .continuation(QueryHash::of(&request), request.cursor.as_deref(), || {
                first_window(request.since_ms, page.until)
            })
            .await?;
        let (Some(since), Some(until)) = (continuation.window.since(), continuation.window.until())
        else {
            return Err(RequestError::CursorMismatch);
        };
        let scan = scan(order, continuation.position())?;
        let filter = StatesFilter {
            service: mem::take(&mut request.service),
            category: mem::take(&mut request.category),
            meta: mem::take(&mut request.meta),
            since,
            until,
            payload,
        };
        let lookup = format!("states of {}/{}", filter.service, filter.category);
        // One row more than the page holds tells that the page is cut.
        let found = self
            .states
            .states(filter, page.limit.saturating_add(1), scan)
            .await;
        log_problems(&found.problems, lookup);
        let cursors = self.cursors(&continuation, found.oldest, order.is_some())?;
        Ok(found.into_responses(page.limit, &cursors))
    }
}

/// The window of the first page: from `since_ms` to `until`, by default
/// the day of states before now.
fn first_window(since_ms: Option<i64>, until: Option<Timestamp>) -> Result<Window, RequestError> {
    let until = until.unwrap_or_else(now);
    let since = match since_ms {
        Some(since_ms) => time(TimeField::Since, since_ms)?,
        None => {
            let since = until.checked_sub(DEFAULT_WINDOW).unwrap_or(Timestamp::MIN);
            on_a_day(TimeField::Since, since)?
        }
    };
    if since > until {
        return Err(RequestError::SinceAfterUntil { since, until });
    }
    Ok(Window::new(Some(since), Some(until))?)
}

/// The scan in `order`, after `position` if given.
fn scan(order: Option<TsOrder>, position: Option<Position>) -> Result<StatesScan, RequestError> {
    match (order, position) {
        (None, None) => Ok(StatesScan::AsRead { from: None }),
        (None, Some(Position::Unordered(row))) => Ok(StatesScan::AsRead { from: Some(row) }),
        (Some(order), None) => Ok(StatesScan::ByTime { order, from: None }),
        (Some(order), Some(Position::Ordered { ts, row })) => Ok(StatesScan::ByTime {
            order,
            from: Some(TimeKey {
                time: order.key(ts),
                row,
            }),
        }),
        _ => Err(RequestError::CursorMismatch),
    }
}

/// Now, in whole milliseconds like each stored time. Thus the window
/// starts and ends on a time that a row can have.
fn now() -> Timestamp {
    Timestamp::from_millisecond(Timestamp::now().as_millisecond())
        .expect("now, in milliseconds, is a time")
}
