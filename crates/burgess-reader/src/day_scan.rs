//! Reading the files of a range of days, newest day first, until enough
//! rows are found.
//!
//! The refs lookup and the states lookup do not know on which days their
//! rows are, thus they read each day of a window. Most of the time is spent
//! on round trips to the store. Thus the manifests of the next days are read
//! ahead, and the files are read across days, so that the reads of one day
//! do not wait for the reads of the day before to complete.

use std::iter;
use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName};
use futures::StreamExt;
use jiff::civil::Date;
use jiff::{Span, Timestamp};

use crate::manifests::{DayFlushes, FlushGroup, ManifestCache, ManifestReadError};

/// A walk over days, with the reads it does at the same time.
pub(crate) struct DayScan<'a> {
    /// The manifests of the days.
    pub(crate) manifests: &'a ManifestCache,
    /// Days whose manifests are read ahead of the files.
    pub(crate) concurrent_days: NonZeroUsize,
    /// Files read at the same time, across days.
    pub(crate) concurrent_files: NonZeroUsize,
}

/// The rows a walk found, and what it could not read.
///
/// A walk continues past a day or a file that it cannot read: an
/// investigation wants all rows that can still be found, and the problems
/// tell which can be missing.
#[derive(Debug)]
pub struct Found<R, E> {
    /// Newest day first, newest flush first in a day, and in the order that
    /// the read of a file gives them.
    pub rows: Vec<R>,
    /// Each manifest or file that could not be read, in walk order.
    pub problems: Vec<E>,
}

impl<R, E> Default for Found<R, E> {
    fn default() -> Self {
        Found {
            rows: Vec::new(),
            problems: Vec::new(),
        }
    }
}

/// A step of a walk: a file to read, or a manifest that could not be read.
enum Step<E> {
    File(ObjectName, FileStats),
    Problem(E),
}

impl DayScan<'_> {
    /// The rows that `read` gives from the files that `pick` selects of each
    /// of `days`, until `wanted` rows are found.
    pub(crate) async fn scan<R, E, F>(
        &self,
        days: Vec<Day>,
        pick: impl Fn(&FlushGroup) -> Option<(ObjectName, FileStats)>,
        read: impl Fn(ObjectName, FileStats) -> F,
        wanted: NonZeroUsize,
    ) -> Found<R, E>
    where
        E: From<ManifestReadError>,
        F: Future<Output = Result<Vec<R>, E>>,
    {
        let read = &read;
        let mut reads = futures::stream::iter(days)
            .map(|day| self.manifests.day(day))
            .buffered(self.concurrent_days.get())
            .flat_map(|manifest| futures::stream::iter(steps(manifest, &pick)))
            .map(|step| async move {
                match step {
                    Step::File(file, stats) => read(file, stats).await,
                    Step::Problem(problem) => Err(problem),
                }
            })
            .buffered(self.concurrent_files.get());
        let mut found = Found::default();
        while let Some(read) = reads.next().await {
            match read {
                Ok(rows) => found.rows.extend(rows),
                Err(problem) => found.problems.push(problem),
            }
            if found.rows.len() >= wanted.get() {
                break;
            }
        }
        found
    }
}

/// The files of a day that `pick` selects, newest flush first, or the
/// problem of its manifest.
fn steps<E: From<ManifestReadError>>(
    manifest: Result<Option<Arc<DayFlushes>>, ManifestReadError>,
    pick: impl Fn(&FlushGroup) -> Option<(ObjectName, FileStats)>,
) -> Vec<Step<E>> {
    match manifest {
        Ok(Some(flushes)) => flushes
            .groups()
            .iter()
            .rev()
            .filter_map(pick)
            .map(|(file, stats)| Step::File(file, stats))
            .collect(),
        Ok(None) => Vec::new(),
        Err(error) => vec![Step::Problem(error.into())],
    }
}

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
