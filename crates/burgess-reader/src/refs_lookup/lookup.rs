//! Looking references up across days.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{Day, FileStats, ObjectName, RefsBloom, RefsKey};

use super::FoundRefs;
use super::error::UnboundedWalk;
use super::query::RefsQuery;
use crate::blooms::BloomReader;
use crate::cursor::RowPosition;
use crate::day_scan::{DayGate, DayScan, ScanConfig, days_back, newest_day, oldest_day};
use crate::manifests::{FlushGroup, ManifestCache};
use crate::refs_file::RefsFileReader;

/// Finds the references to an outside id across the days kept.
pub struct RefsLookup {
    manifests: Arc<ManifestCache>,
    blooms: BloomReader,
    files: RefsFileReader,
    config: ScanConfig,
}

impl RefsLookup {
    /// A lookup over the days `manifests` lists, with the filters that
    /// `blooms` reads, and the files `files` reads.
    pub fn new(
        manifests: Arc<ManifestCache>,
        blooms: BloomReader,
        files: RefsFileReader,
        config: ScanConfig,
    ) -> Self {
        RefsLookup {
            manifests,
            blooms,
            files,
            config,
        }
    }

    /// The references that pass `query`, after the row `from` if given,
    /// until `wanted` are found, and what could not be read.
    ///
    /// The order of the rows is not defined, thus the walk can stop when it
    /// has `wanted` rows. It walks newest first, from the day after `until`,
    /// but not after tomorrow by this host's clock. It stops at the day of
    /// `since`, less the configured days before it, or at the oldest day
    /// kept. A refs file without rows is not opened.
    ///
    /// The references filter of each day is read first: a day that it
    /// rules out costs that one request, and its manifest is not read.
    /// Otherwise each refs file is opened only when the filter can hold the
    /// reference in it. A day without a filter that can be used is read
    /// whole. The filter is read before the manifest, thus it holds the
    /// keys of each file that was listed when the lookup read it.
    ///
    /// The last of the `wanted` rows only tells that the answer goes on,
    /// thus a file that cannot be read after the rows before it is left to
    /// the next page, which reads it again.
    pub async fn refs(
        &self,
        query: &RefsQuery,
        from: Option<RowPosition>,
        wanted: NonZeroUsize,
    ) -> Result<FoundRefs, UnboundedWalk> {
        let days = self.days(query)?;
        let scan = DayScan {
            manifests: &self.manifests,
            config: self.config,
        };
        let day_key = RefsKey::day(query.key.ref_type(), query.key.domain_id());
        let gate = |day: Day| {
            let day_key = &day_key;
            async move {
                match self.blooms.refs(day).await {
                    Some(filter) if !filter.may_contain(day_key) => DayGate::Skip,
                    filter => DayGate::Read(filter),
                }
            }
        };
        let pick = |group: &FlushGroup, filter: &Option<RefsBloom>| {
            let file_key =
                || RefsKey::file(group.nnnn(), query.key.ref_type(), query.key.domain_id());
            let may_hold = group.refs().rows > 0
                && filter
                    .as_ref()
                    .is_none_or(|filter| filter.may_contain(&file_key()));
            may_hold.then(|| (group.refs_file(), group.refs()))
        };
        let read = |file: ObjectName, stats: FileStats, after: Option<u64>| async move {
            let rows = self.files.refs(&file, stats, &query.key, after).await?;
            Ok(rows
                .into_iter()
                .filter(|row| query.admits(&row.row))
                .collect())
        };
        Ok(scan.scan(days, from, gate, pick, read, wanted).await)
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
