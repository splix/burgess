//! Looking states up across days.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName};

use super::FoundStates;
use crate::day_scan::{DayScan, ScanConfig, days_back, newest_day, oldest_day};
use crate::manifests::{FlushGroup, ManifestCache};
use crate::ordering::TsOrder;
use crate::states_file::{StatesFileReader, StatesFilter};

/// Finds the states that pass a filter across the days of its window.
pub struct StatesLookup {
    pub(super) manifests: Arc<ManifestCache>,
    pub(super) files: StatesFileReader,
    pub(super) config: ScanConfig,
}

impl StatesLookup {
    /// A lookup over the days `manifests` lists and the files `files` reads.
    pub fn new(manifests: Arc<ManifestCache>, files: StatesFileReader, config: ScanConfig) -> Self {
        StatesLookup {
            manifests,
            files,
            config,
        }
    }

    /// The first `wanted` states that pass `filter`, in `order` or in the
    /// order they are read, and what could not be read.
    ///
    /// The walk reads the days from the day after `until`, but not after
    /// tomorrow by this host's clock, to the day of `since`, less the
    /// configured days before it, but not before the oldest day kept. It
    /// opens only the states files whose time bounds touch the window.
    pub async fn states(
        &self,
        filter: StatesFilter,
        wanted: NonZeroUsize,
        order: Option<TsOrder>,
    ) -> FoundStates {
        let filter = Arc::new(filter);
        match order {
            None => self.unordered(filter, wanted).await,
            Some(order) => self.ordered(filter, wanted, order).await,
        }
    }

    /// The order of the rows is not defined, thus the walk can stop when it
    /// has `wanted` rows, and a file read stops there too. It walks newest
    /// day first.
    async fn unordered(&self, filter: Arc<StatesFilter>, wanted: NonZeroUsize) -> FoundStates {
        let days = self.days(&filter);
        let scan = DayScan {
            manifests: &self.manifests,
            config: self.config,
        };
        let pick = |group: &FlushGroup| {
            group
                .may_hold_times(filter.since, filter.until)
                .then(|| (group.states_file(), group.states()))
        };
        let read = |file: ObjectName, stats: FileStats| {
            let filter = &filter;
            async move {
                let states = self
                    .files
                    .states(&file, stats, filter, wanted, None)
                    .await?;
                Ok(states)
            }
        };
        scan.scan(days, pick, read, wanted).await
    }

    /// The days to visit, newest first. No day when a time of the window or
    /// today is on a day that a `Day` cannot name, which the request checks
    /// prevent for the window.
    pub(super) fn days(&self, filter: &StatesFilter) -> Vec<Day> {
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
