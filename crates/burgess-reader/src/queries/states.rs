//! The ListStates lookup.

use std::mem;

use burgess_api::grpc::{ListStatesRequest, ListStatesResponse};
use jiff::{SignedDuration, Timestamp};

use super::error::{RequestError, TimeField};
use super::options::{PageOptions, on_a_day, time};
use super::{Queries, log_problems, non_empty};
use crate::states_file::StatesFilter;

/// The window of a request without `since`: the day of states before
/// `until`.
const DEFAULT_WINDOW: SignedDuration = SignedDuration::from_hours(24);

impl Queries {
    /// The messages of the answer to a ListStates request.
    ///
    /// A window without an end is made absolute here, one time, thus all
    /// days and files of the lookup use the same window.
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
        if non_empty(request.r#where.as_deref()).is_some() {
            return Err(RequestError::Where);
        }
        let page = PageOptions::from(&request).check(&self.limits)?;
        let until = page.until.unwrap_or_else(now);
        let since = match request.since_ms {
            Some(since_ms) => time(TimeField::Since, since_ms)?,
            None => {
                let since = until.checked_sub(DEFAULT_WINDOW).unwrap_or(Timestamp::MIN);
                on_a_day(TimeField::Since, since)?
            }
        };
        if since > until {
            return Err(RequestError::SinceAfterUntil { since, until });
        }
        let filter = StatesFilter {
            service: mem::take(&mut request.service),
            category: mem::take(&mut request.category),
            meta: mem::take(&mut request.meta),
            since,
            until,
        };
        let lookup = format!("states of {}/{}", filter.service, filter.category);
        // One row more than the page holds tells that the page is cut.
        let found = self
            .states
            .states(filter, page.limit.saturating_add(1))
            .await;
        log_problems(&found.problems, lookup);
        Ok(found.into_responses(page.limit))
    }
}

/// Now, in whole milliseconds like each stored time. Thus the window
/// starts and ends on a time that a row can have.
fn now() -> Timestamp {
    Timestamp::from_millisecond(Timestamp::now().as_millisecond())
        .expect("now, in milliseconds, is a time")
}
