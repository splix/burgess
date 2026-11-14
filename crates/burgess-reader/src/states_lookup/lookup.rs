//! Looking states up across days.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName};

use super::FoundStates;
use super::scan::StatesScan;
use crate::cursor::RowPosition;
use crate::day_scan::{DayScan, ScanConfig, days_back, newest_day, oldest_day};
use crate::manifests::{FlushGroup, ManifestCache};
use crate::states_file::{ScanOrder, StatesFileReader, StatesFilter};

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

    /// The first `wanted` states that pass `filter`, as `scan` orders them
    /// and after the state it gives, and what could not be read.
    ///
    /// The walk reads the days from the day after `until`, but not after
    /// tomorrow by this host's clock, to the day of `since`, less the
    /// configured days before it, but not before the oldest day kept. It
    /// opens only the states files whose time bounds touch the window.
    ///
    /// The last of the `wanted` states only tells that the answer goes on,
    /// thus a file that cannot be read and can hide only states after it is
    /// left to the next page, which reads it again.
    pub async fn states(
        &self,
        filter: StatesFilter,
        wanted: NonZeroUsize,
        scan: StatesScan,
    ) -> FoundStates {
        let filter = Arc::new(filter);
        match scan {
            StatesScan::AsRead { from } => self.unordered(filter, wanted, from).await,
            StatesScan::ByTime { order, from } => self.ordered(filter, wanted, order, from).await,
        }
    }

    /// The order of the rows is not defined, thus the walk can stop when it
    /// has `wanted` rows, and a file read stops there too. It walks newest
    /// day first.
    async fn unordered(
        &self,
        filter: Arc<StatesFilter>,
        wanted: NonZeroUsize,
        from: Option<RowPosition>,
    ) -> FoundStates {
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
        let read = |file: ObjectName, stats: FileStats, after: Option<u64>| {
            let filter = &filter;
            async move {
                let order = ScanOrder::InFile { after };
                let states = self
                    .files
                    .states(&file, stats, filter, wanted, order)
                    .await?;
                Ok(states)
            }
        };
        scan.scan(days, from, pick, read, wanted).await
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
