//! The walk over the days and the files.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName};
use futures::{Stream, StreamExt};

use super::config::ScanConfig;
use super::problems::WalkProblems;
use crate::cursor::{Given, Positioned, RowPosition, Walk};
use crate::manifests::{DayFlushes, FlushGroup, ManifestCache, ManifestReadError};
use crate::parquet_file::Numbered;

/// A walk over days, with the reads it does at the same time.
pub(crate) struct DayScan<'a> {
    /// The manifests of the days.
    pub(crate) manifests: &'a ManifestCache,
    /// How many reads are done at the same time.
    pub(crate) config: ScanConfig,
}

/// The rows a walk found, and what it could not read.
///
/// A walk continues past a day or a file that it cannot read: an
/// investigation wants all rows that can still be found, and the problems
/// tell which can be missing.
#[derive(Debug)]
pub struct Found<R, E> {
    /// In the order of the lookup: as the walk reads them, newest day and
    /// newest flush first, or sorted, when the lookup has an order.
    pub rows: Vec<R>,
    /// Each manifest or file that could not be read and can hide a row of
    /// the page, in walk order.
    pub problems: Vec<E>,
    /// The oldest day that the walk reads. `None` when it reads no day.
    pub oldest: Option<Day>,
}

impl<R, E> Default for Found<R, E> {
    fn default() -> Self {
        Found {
            rows: Vec::new(),
            problems: Vec::new(),
            oldest: None,
        }
    }
}

/// A step of a walk: a file to read, or a manifest that could not be read.
enum Step<E> {
    File {
        group: FlushGroup,
        file: ObjectName,
        stats: FileStats,
        /// The last row of the file that a page gave.
        after: Option<u64>,
    },
    Problem(E),
}

impl DayScan<'_> {
    /// The rows that `read` gives from the files that `pick` selects of each
    /// of `days`, after the row `from` if given, until `wanted` rows are
    /// found. `read` gets the last row of the file that a page gave.
    ///
    /// The last of the `wanted` rows only tells that the answer goes on,
    /// thus a problem that can hide only rows after it is left to the next
    /// page.
    pub(crate) async fn scan<R, E, F>(
        &self,
        days: Vec<Day>,
        from: Option<RowPosition>,
        pick: impl Fn(&FlushGroup) -> Option<(ObjectName, FileStats)>,
        read: impl Fn(ObjectName, FileStats, Option<u64>) -> F,
        wanted: NonZeroUsize,
    ) -> Found<Positioned<R>, E>
    where
        E: From<ManifestReadError>,
        F: Future<Output = Result<Vec<Numbered<R>>, E>>,
    {
        let read = &read;
        let oldest = days.last().copied();
        // The days are newest first, thus a page gave all rows of the days
        // after the day of `from`.
        let days = days
            .into_iter()
            .skip_while(|day| from.is_some_and(|from| *day > from.day))
            .collect();
        let mut reads = self
            .manifests(days)
            .flat_map(|manifest| futures::stream::iter(steps(manifest, &pick, from)))
            .map(|step| async move {
                match step {
                    Step::File {
                        group,
                        file,
                        stats,
                        after,
                    } => {
                        let rows = read(file, stats, after).await?;
                        Ok(rows
                            .into_iter()
                            .map(|row| Positioned::in_flush(&group, row))
                            .collect::<Vec<_>>())
                    }
                    Step::Problem(problem) => Err(problem),
                }
            })
            .buffered(self.config.concurrent_files.get());
        let mut rows = Vec::new();
        let mut problems = WalkProblems::new(Some(wanted));
        while let Some(read) = reads.next().await {
            match read {
                Ok(read) => rows.extend(read),
                Err(problem) => problems.push(problem, rows.len()),
            }
            if rows.len() >= wanted.get() {
                break;
            }
        }
        Found {
            problems: problems.of_page(rows.len()),
            rows,
            oldest,
        }
    }
}

impl DayScan<'_> {
    /// What `pick` takes of each flush of `days`, newest day and newest
    /// flush first, and the manifests that could not be read. No file is
    /// read, thus a caller can choose the order of the files.
    pub(crate) async fn picked<T, E: From<ManifestReadError>>(
        &self,
        days: Vec<Day>,
        pick: impl Fn(&FlushGroup) -> Option<T>,
    ) -> (Vec<T>, Vec<E>) {
        let mut manifests = std::pin::pin!(self.manifests(days));
        let (mut picked, mut problems) = (Vec::new(), Vec::new());
        while let Some(manifest) = manifests.next().await {
            match picks(manifest, &pick) {
                Ok(picks) => picked.extend(picks),
                Err(error) => problems.push(error.into()),
            }
        }
        (picked, problems)
    }

    /// The manifests of `days`, in their order, read ahead.
    fn manifests(
        &self,
        days: Vec<Day>,
    ) -> impl Stream<Item = Result<Option<Arc<DayFlushes>>, ManifestReadError>> + '_ {
        futures::stream::iter(days)
            .map(|day| self.manifests.day(day))
            .buffered(self.config.concurrent_days.get())
    }
}

/// What `pick` takes of each flush of a day, newest flush first.
fn picks<T>(
    manifest: Result<Option<Arc<DayFlushes>>, ManifestReadError>,
    pick: impl Fn(&FlushGroup) -> Option<T>,
) -> Result<Vec<T>, ManifestReadError> {
    Ok(match manifest? {
        Some(flushes) => flushes.groups().iter().rev().filter_map(pick).collect(),
        None => Vec::new(),
    })
}

/// The files of a day that `pick` selects and that have rows after `from`,
/// newest flush first, or the problem of its manifest.
fn steps<E: From<ManifestReadError>>(
    manifest: Result<Option<Arc<DayFlushes>>, ManifestReadError>,
    pick: impl Fn(&FlushGroup) -> Option<(ObjectName, FileStats)>,
    from: Option<RowPosition>,
) -> Vec<Step<E>> {
    let files = picks(manifest, |group| {
        let after = Given::read_after(group, from, Walk::NewestFirst)?;
        let (file, stats) = pick(group)?;
        Some(Step::File {
            group: group.clone(),
            file,
            stats,
            after,
        })
    });
    files.unwrap_or_else(|error| vec![Step::Problem(error.into())])
}
