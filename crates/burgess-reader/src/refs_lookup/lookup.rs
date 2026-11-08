//! Looking references up across days.

use std::iter;
use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName};
use futures::StreamExt;
use jiff::Timestamp;

use super::config::RefsLookupConfig;
use super::error::{RefsLookupError, UnboundedWalk};
use super::found::FoundRefs;
use super::query::RefsQuery;
use crate::manifests::{DayFlushes, ManifestCache, ManifestReadError};
use crate::refs_file::RefsFileReader;

/// Finds the references to an outside id across the days kept.
pub struct RefsLookup {
    manifests: Arc<ManifestCache>,
    files: RefsFileReader,
    config: RefsLookupConfig,
}

/// A step of a walk: a file to read, or a manifest that could not be read.
enum Step {
    File(ObjectName, FileStats),
    Problem(RefsLookupError),
}

impl RefsLookup {
    /// A lookup over the days `manifests` lists and the files `files` reads.
    pub fn new(
        manifests: Arc<ManifestCache>,
        files: RefsFileReader,
        config: RefsLookupConfig,
    ) -> Self {
        RefsLookup {
            manifests,
            files,
            config,
        }
    }

    /// The references that pass `query`, until `wanted` are found, and what
    /// could not be read.
    ///
    /// The order of the rows is not defined, thus the walk can stop when it
    /// has `wanted` rows. It walks newest first, from the day after `until`,
    /// but not after tomorrow by this host's clock, since the writer can
    /// have stored nothing later. It stops at the day of `since`, less the
    /// configured days before it, or at the oldest day kept. A refs file
    /// without rows is not opened.
    ///
    /// The manifests of the next days are read ahead, and the files are
    /// read across days, thus the reads of one day do not wait for the
    /// reads of the day before to complete.
    pub async fn refs(
        &self,
        query: &RefsQuery,
        wanted: NonZeroUsize,
    ) -> Result<FoundRefs, UnboundedWalk> {
        let days = self.days(query)?;
        let steps = futures::stream::iter(days)
            .map(|day| self.manifests.day(day))
            .buffered(self.config.concurrent_days.get())
            .flat_map(|manifest| futures::stream::iter(steps(manifest)));
        let mut reads = steps
            .map(|step| async move {
                match step {
                    Step::File(file, stats) => self
                        .files
                        .refs(&file, stats, &query.key)
                        .await
                        .map_err(RefsLookupError::from),
                    Step::Problem(problem) => Err(problem),
                }
            })
            .buffered(self.config.concurrent_files.get());
        let mut found = FoundRefs::default();
        while let Some(read) = reads.next().await {
            match read {
                Ok(rows) => found
                    .rows
                    .extend(rows.into_iter().filter(|row| query.admits(row))),
                Err(problem) => found.problems.push(problem),
            }
            if found.rows.len() >= wanted.get() {
                break;
            }
        }
        Ok(found)
    }

    /// The days to visit, newest first. None when today is a day that a
    /// `Day` cannot name.
    fn days(&self, query: &RefsQuery) -> Result<Vec<Day>, UnboundedWalk> {
        let Ok(today) = Day::utc(Timestamp::now()) else {
            return Ok(Vec::new());
        };
        let newest = query.newest_day(today.next().unwrap_or(today));
        let oldest = query
            .oldest_day(self.manifests.oldest_kept(), self.config.days_before)
            .ok_or(UnboundedWalk)?;
        Ok(iter::successors(Some(newest), |day| day.previous())
            .take_while(|day| *day >= oldest)
            .collect())
    }
}

/// The refs files of a day with rows, newest flush first, or the problem of
/// its manifest.
fn steps(manifest: Result<Option<Arc<DayFlushes>>, ManifestReadError>) -> Vec<Step> {
    match manifest {
        Ok(Some(flushes)) => flushes
            .groups()
            .iter()
            .rev()
            .filter(|group| group.refs().rows > 0)
            .map(|group| Step::File(group.refs_file(), group.refs()))
            .collect(),
        Ok(None) => Vec::new(),
        Err(error) => vec![Step::Problem(error.into())],
    }
}
