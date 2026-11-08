//! Looking states up across days.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName};

use super::FoundStates;
use super::config::StatesLookupConfig;
use crate::day_scan::{DayScan, days_back, newest_day, oldest_day};
use crate::manifests::{FlushGroup, ManifestCache};
use crate::states_file::{StatesFileReader, StatesFilter};

/// Finds the states that pass a filter across the days of its window.
pub struct StatesLookup {
    manifests: Arc<ManifestCache>,
    files: StatesFileReader,
    config: StatesLookupConfig,
}

impl StatesLookup {
    /// A lookup over the days `manifests` lists and the files `files` reads.
    pub fn new(
        manifests: Arc<ManifestCache>,
        files: StatesFileReader,
        config: StatesLookupConfig,
    ) -> Self {
        StatesLookup {
            manifests,
            files,
            config,
        }
    }

    /// The states that pass `filter`, until `wanted` are found, and what
    /// could not be read.
    ///
    /// The order of the rows is not defined, thus the walk can stop when it
    /// has `wanted` rows, and a file read stops there too. It walks newest
    /// first, from the day after `until`, but not after tomorrow by this
    /// host's clock, to the day of `since`, less the configured days before
    /// it, but not before the oldest day kept. It opens only the states
    /// files whose time bounds touch the window.
    pub async fn states(&self, filter: StatesFilter, wanted: NonZeroUsize) -> FoundStates {
        let days = self.days(&filter);
        let filter = Arc::new(filter);
        let scan = DayScan {
            manifests: &self.manifests,
            concurrent_days: self.config.concurrent_days,
            concurrent_files: self.config.concurrent_files,
        };
        let pick = |group: &FlushGroup| {
            group
                .may_hold_times(filter.since, filter.until)
                .then(|| (group.states_file(), group.states()))
        };
        let read = |file: ObjectName, stats: FileStats| {
            let filter = &filter;
            async move {
                let states = self.files.states(&file, stats, filter, wanted).await?;
                Ok(states)
            }
        };
        scan.scan(days, pick, read, wanted).await
    }

    /// The days to visit, newest first. None when a time of the window or
    /// today is on a day that a `Day` cannot name.
    fn days(&self, filter: &StatesFilter) -> Vec<Day> {
        let oldest = oldest_day(
            Some(filter.since),
            self.manifests.oldest_kept(),
            self.config.days_before,
        );
        match (newest_day(Some(filter.until)), oldest) {
            (Some(newest), Some(oldest)) => days_back(newest, oldest),
            _ => Vec::new(),
        }
    }
}
