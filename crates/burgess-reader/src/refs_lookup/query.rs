//! What a refs lookup searches for.

use burgess_core::storage::{Day, RefRow};
use jiff::civil::Date;
use jiff::{Span, Timestamp};

use crate::refs_file::RefKey;

/// The references to look up: an outside id, and filters on the rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefsQuery {
    /// The outside id.
    pub key: RefKey,
    /// Only references that this service recorded.
    pub service: Option<String>,
    /// Only references recorded in this category.
    pub category: Option<String>,
    /// Only references at this time or later, by the producer clock.
    pub since: Option<Timestamp>,
    /// Only references at this time or earlier, by the producer clock.
    pub until: Option<Timestamp>,
}

impl RefsQuery {
    /// Whether `row`, a reference to the key, passes the filters.
    pub(super) fn admits(&self, row: &RefRow) -> bool {
        let ts = row.time.saturating_timestamp();
        self.service
            .as_ref()
            .is_none_or(|service| *service == row.service)
            && self
                .category
                .as_ref()
                .is_none_or(|category| row.category.as_ref() == Some(category))
            && self.since.is_none_or(|since| since <= ts)
            && self.until.is_none_or(|until| ts <= until)
    }

    /// The first day of the walk, which is the newest day that can hold a
    /// reference that passes the filters, but no later than `latest`.
    ///
    /// The writer stores a reference on the day of the flush, which can be
    /// the day after its time. Thus the walk starts on the day after
    /// `until`.
    pub(super) fn newest_day(&self, latest: Day) -> Day {
        self.until
            .and_then(|until| Day::utc(until).ok())
            .and_then(Day::next)
            .map_or(latest, |day| day.min(latest))
    }

    /// The last day of the walk: `days_before` days before the day of
    /// `since`, but not before `oldest`, the oldest day kept. `None` when
    /// neither limits the walk.
    pub(super) fn oldest_day(&self, oldest: Option<Day>, days_before: u32) -> Option<Day> {
        let since = self
            .since
            .and_then(|since| Day::utc(since).ok())
            .map(|since| before(since, days_before));
        match (since, oldest) {
            (Some(since), Some(oldest)) => Some(since.max(oldest)),
            (since, oldest) => since.or(oldest),
        }
    }
}

/// The day `days` before `day`, or `day` when that day cannot be named.
fn before(day: Day, days: u32) -> Day {
    Span::new()
        .try_days(i64::from(days))
        .ok()
        .and_then(|span| Date::from(day).checked_sub(span).ok())
        .and_then(|date| Day::try_from(date).ok())
        .unwrap_or(day)
}
