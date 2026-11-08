//! The walk over the days and the files.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName};
use futures::StreamExt;

use super::config::ScanConfig;
use crate::manifests::{DayFlushes, FlushGroup, ManifestCache, ManifestReadError};

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
            .buffered(self.config.concurrent_days.get())
            .flat_map(|manifest| futures::stream::iter(steps(manifest, &pick)))
            .map(|step| async move {
                match step {
                    Step::File(file, stats) => read(file, stats).await,
                    Step::Problem(problem) => Err(problem),
                }
            })
            .buffered(self.config.concurrent_files.get());
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
