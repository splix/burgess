//! Which days a walk reads.

use std::iter;

use burgess_core::storage::Day;
use jiff::civil::Date;
use jiff::{Span, Timestamp};

/// The first day of a walk for rows timed up to `until`: the day after it,
/// since the writer stores a row on the day of its flush, which can be the
/// day after its time. But not after tomorrow by this host's clock, since
/// the writer can have stored nothing later, which is also the first day
/// without `until`. `None` when today is a day that a `Day` cannot name.
pub(crate) fn newest_day(until: Option<Timestamp>) -> Option<Day> {
    let today = Day::utc(Timestamp::now()).ok()?;
    let latest = today.next().unwrap_or(today);
    Some(
        until
            .and_then(|until| Day::utc(until).ok())
            .and_then(Day::next)
            .map_or(latest, |day| day.min(latest)),
    )
}

/// The last day of a walk for rows timed from `since`: `days_before` days
/// before its day, since a producer clock ahead of the writer clock across
/// midnight stores a row on the day before its time. But not before
/// `oldest`, the oldest day kept, which is also the last day without
/// `since`. `None` when neither ends the walk.
pub(crate) fn oldest_day(
    since: Option<Timestamp>,
    oldest: Option<Day>,
    days_before: u32,
) -> Option<Day> {
    let since = since
        .and_then(|since| Day::utc(since).ok())
        .map(|since| before(since, days_before));
    match (since, oldest) {
        (Some(since), Some(oldest)) => Some(since.max(oldest)),
        (since, oldest) => since.or(oldest),
    }
}

/// The days from `newest` back to `oldest`, newest first.
pub(crate) fn days_back(newest: Day, oldest: Day) -> Vec<Day> {
    iter::successors(Some(newest), |day| day.previous())
        .take_while(|day| *day >= oldest)
        .collect()
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
