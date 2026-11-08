//! Looking references up across days.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName};

use super::FoundRefs;
use super::error::UnboundedWalk;
use super::query::RefsQuery;
use crate::day_scan::{DayScan, ScanConfig, days_back, newest_day, oldest_day};
use crate::manifests::{FlushGroup, ManifestCache};
use crate::refs_file::RefsFileReader;

/// Finds the references to an outside id across the days kept.
pub struct RefsLookup {
    manifests: Arc<ManifestCache>,
    files: RefsFileReader,
    config: ScanConfig,
}

impl RefsLookup {
    /// A lookup over the days `manifests` lists and the files `files` reads.
    pub fn new(manifests: Arc<ManifestCache>, files: RefsFileReader, config: ScanConfig) -> Self {
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
    /// but not after tomorrow by this host's clock. It stops at the day of
    /// `since`, less the configured days before it, or at the oldest day
    /// kept. A refs file without rows is not opened.
    pub async fn refs(
        &self,
        query: &RefsQuery,
        wanted: NonZeroUsize,
    ) -> Result<FoundRefs, UnboundedWalk> {
        let days = self.days(query)?;
        let scan = DayScan {
            manifests: &self.manifests,
            config: self.config,
        };
        let pick =
            |group: &FlushGroup| (group.refs().rows > 0).then(|| (group.refs_file(), group.refs()));
        let read = |file: ObjectName, stats: FileStats| async move {
            let rows = self.files.refs(&file, stats, &query.key).await?;
            Ok(rows.into_iter().filter(|row| query.admits(row)).collect())
        };
        Ok(scan.scan(days, pick, read, wanted).await)
    }

    /// The days to visit, newest first. None when today is a day that a
    /// `Day` cannot name.
    fn days(&self, query: &RefsQuery) -> Result<Vec<Day>, UnboundedWalk> {
        let Some(newest) = newest_day(query.until) else {
            return Ok(Vec::new());
        };
        let oldest = oldest_day(
            query.since,
            self.manifests.oldest_kept(),
            self.config.days_before,
        )
        .ok_or(UnboundedWalk)?;
        Ok(days_back(newest, oldest))
    }
}
