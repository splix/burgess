//! Looking a tree up across days.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::flow::{FlowId, TreeId};
use burgess_core::storage::{Day, StateRow};
use futures::StreamExt;
use jiff::civil::Date;
use jiff::{Span, Timestamp};

use super::config::TreeLookupConfig;
use super::found::FoundTree;
use super::walk::{DayOutcome, DayWalk, WalkRange};
use crate::cursor::{Given, Positioned, RowPosition, Walk};
use crate::day_scan::WalkProblems;
use crate::manifests::{FlushGroup, ManifestCache};
use crate::parquet_file::Numbered;
use crate::states_file::{StatesFileReader, StatesReadError};
use crate::states_lookup::StatesLookupError;

/// Finds every row of a tree, or of a flow and the flows below it, across
/// the days they were written on.
pub struct TreeLookup {
    manifests: Arc<ManifestCache>,
    files: StatesFileReader,
    config: TreeLookupConfig,
}

impl TreeLookup {
    /// A lookup over the days `manifests` lists and the files `files` reads.
    pub fn new(
        manifests: Arc<ManifestCache>,
        files: StatesFileReader,
        config: TreeLookupConfig,
    ) -> Self {
        TreeLookup {
            manifests,
            files,
            config,
        }
    }

    /// The rows of `tree` that can be read, states and splits, after the
    /// row `from` if given, and what could not be read. With `wanted`, the
    /// walk stops after the first day that brings the rows to that count,
    /// and the last of them only tells that the answer goes on, as in a
    /// day scan.
    ///
    /// A walk that continues after `from` starts on its day. That day held
    /// the tree, thus the walk then goes on as the walk of the first page
    /// did.
    ///
    /// The walk starts on the id's day, or the configured days before it,
    /// but never before the oldest day still within retention.
    ///
    /// A day is negative when it has files and none of their bounds includes
    /// the tree, and positive when some do, whether or not those files turn
    /// out to hold the tree's rows. A day without files, or whose manifest
    /// cannot be read, is neither. Without `until` the walk goes on past the
    /// configured slack of negative days in a row and stops on the next
    /// negative day after them. With it the walk
    /// visits every day up to the day after `until`; `until` picks days only
    /// and filters no rows. Either way the walk ends at tomorrow by this
    /// host's clock, since the writer can have stored nothing later.
    pub async fn tree(
        &self,
        tree: TreeId,
        until: Option<Timestamp>,
        from: Option<RowPosition>,
        wanted: Option<NonZeroUsize>,
    ) -> FoundTree {
        let Some(range) = self.range(tree, from) else {
            return FoundTree::default();
        };
        let Ok(until) = until.map(Day::utc).transpose() else {
            return FoundTree::default();
        };
        let oldest = range.first;
        let mut walk = DayWalk::new(range, until, self.config.stop_slack_days);
        let mut rows = Vec::new();
        let mut problems = WalkProblems::new(wanted);
        while let Some(day) = walk.next_day() {
            let outcome = self.visit(day, tree, from, &mut rows, &mut problems).await;
            walk.visited(outcome);
            if wanted.is_some_and(|wanted| rows.len() >= wanted.get()) {
                break;
            }
        }
        FoundTree {
            problems: problems.of_page(rows.len()),
            rows,
            oldest: Some(oldest),
        }
    }

    /// Every row of `flow` and of the flows split off below it, at any
    /// depth, and what could not be read.
    ///
    /// The whole tree is read as [`Self::tree`] reads it, since only the
    /// split rows tell which flows lie below, and they can be filed on any
    /// day the tree was written on. So the flows found are those whose
    /// splits fall within the days walked: with `until`, a child whose
    /// split was filed after them is left out, even when its own rows were
    /// filed in time.
    pub async fn flow(&self, flow: FlowId, until: Option<Timestamp>) -> FoundTree {
        let tree = self.tree(flow.tree(), until, None, None).await;
        tree.subtree(flow.subflow())
    }

    /// The days to visit, or `None` when no day can hold the tree: the id's
    /// day lies outside the days a `Day` can name.
    fn range(&self, tree: TreeId, from: Option<RowPosition>) -> Option<WalkRange> {
        let minted = Day::utc(tree.started_at()).ok()?;
        let oldest = self.manifests.oldest_kept();
        let at_least_oldest = |day: Day| oldest.map_or(day, |oldest| day.max(oldest));
        // A start before any day that can be named is before retention too.
        let first = self
            .days_before(minted)
            .map_or(oldest.unwrap_or(minted), at_least_oldest);
        let today = Day::utc(Timestamp::now()).ok()?;
        let first = from.map_or(first, |from| from.day.max(first));
        Some(WalkRange {
            first,
            counted_from: at_least_oldest(minted),
            latest: today.next().unwrap_or(today),
        })
    }

    /// The configured days before `minted`, or `None` when that day is
    /// before any day a `Day` can name.
    fn days_before(&self, minted: Day) -> Option<Day> {
        let span = Span::new()
            .try_days(i64::from(self.config.days_before))
            .ok()?;
        let date = Date::from(minted).checked_sub(span).ok()?;
        Day::try_from(date).ok()
    }

    /// Reads the day's files that may hold `tree`, after the row `from`,
    /// into `rows` and `problems`, and tells what the day said about the
    /// tree.
    async fn visit(
        &self,
        day: Day,
        tree: TreeId,
        from: Option<RowPosition>,
        rows: &mut Vec<Positioned<StateRow>>,
        problems: &mut WalkProblems<StatesLookupError>,
    ) -> DayOutcome {
        let flushes = match self.manifests.day(day).await {
            Ok(Some(flushes)) if !flushes.groups().is_empty() => flushes,
            Ok(_) => return DayOutcome::Unknown,
            Err(error) => {
                problems.push(error.into(), rows.len());
                return DayOutcome::Unknown;
            }
        };
        let holding: Vec<&FlushGroup> = flushes.holding(tree).collect();
        if holding.is_empty() {
            return DayOutcome::Negative;
        }
        let candidates: Vec<(FlushGroup, Option<u64>)> = holding
            .into_iter()
            .filter_map(|group| {
                let after = Given::read_after(group, from, Walk::OldestFirst)?;
                Some((group.clone(), after))
            })
            .collect();
        for (group, read) in self.read(candidates, tree).await {
            match read {
                Ok(read) => rows.extend(
                    read.into_iter()
                        .map(|row| Positioned::in_flush(&group, row)),
                ),
                Err(error) => problems.push(error.into(), rows.len()),
            }
        }
        DayOutcome::Positive
    }

    /// The rows of `tree` in the states file of each of `candidates`, after
    /// the row it gives, in their order.
    async fn read(
        &self,
        candidates: Vec<(FlushGroup, Option<u64>)>,
        tree: TreeId,
    ) -> Vec<(FlushGroup, Result<Vec<Numbered<StateRow>>, StatesReadError>)> {
        futures::stream::iter(candidates)
            .map(|(group, after)| async move {
                let read = self
                    .files
                    .tree(&group.states_file(), group.states(), tree, after)
                    .await;
                (group, read)
            })
            .buffered(self.config.concurrent_files.get())
            .collect()
            .await
    }
}
