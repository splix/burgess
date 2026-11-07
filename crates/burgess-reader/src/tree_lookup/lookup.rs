//! Looking a tree up across days.

use std::sync::Arc;

use burgess_core::flow::TreeId;
use burgess_core::storage::{Day, FileStats, ObjectName, StateRow};
use futures::StreamExt;
use jiff::civil::Date;
use jiff::{Span, Timestamp};

use super::config::TreeLookupConfig;
use super::found::FoundTree;
use super::walk::{DayOutcome, DayWalk, WalkRange};
use crate::manifests::ManifestCache;
use crate::states_file::{StatesFileReader, TreeReadError};

/// Finds every row of a tree across the days it was written on.
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

    /// Every row of `tree` that can be read, states and splits, and what
    /// could not be read.
    ///
    /// The walk starts on the id's day, or the configured days before it,
    /// but never before the oldest day still within retention.
    ///
    /// A day is negative when it has files and none of their bounds includes
    /// the tree, and positive when some do, whether or not those files turn
    /// out to hold the tree's rows. A day without a manifest, or with one
    /// that cannot be read, is neither. Without `until` the walk stops after
    /// the configured slack of negative days in a row. With it the walk
    /// visits every day up to the day after `until`; `until` picks days only
    /// and filters no rows. Either way the walk ends at tomorrow by this
    /// host's clock, since the writer can have stored nothing later.
    pub async fn tree(&self, tree: TreeId, until: Option<Timestamp>) -> FoundTree {
        let mut found = FoundTree::default();
        let Some(mut walk) = self.walk(tree, until) else {
            return found;
        };
        while let Some(day) = walk.next_day() {
            let outcome = self.visit(day, tree, &mut found).await;
            walk.visited(outcome);
        }
        found
    }

    /// The days to visit, or `None` when no day can hold the tree: the id's
    /// day or `until` lies outside the days a `Day` can name.
    fn walk(&self, tree: TreeId, until: Option<Timestamp>) -> Option<DayWalk> {
        let minted = Day::utc(tree.started_at()).ok()?;
        let early = Date::from(minted)
            .checked_sub(Span::new().days(i64::from(self.config.days_before)))
            .ok()
            .and_then(|date| Day::try_from(date).ok())
            .unwrap_or(minted);
        let oldest = self.manifests.oldest_kept();
        let at_least_oldest = |day: Day| oldest.map_or(day, |oldest| day.max(oldest));
        let until = until.map(Day::utc).transpose().ok()?;
        let today = Day::utc(Timestamp::now()).ok()?;
        let range = WalkRange {
            first: at_least_oldest(early),
            counted_from: at_least_oldest(minted),
            latest: today.next().unwrap_or(today),
        };
        Some(DayWalk::new(range, until, self.config.stop_slack_days))
    }

    /// Reads the day's files that may hold `tree` into `found`, and tells
    /// what the day said about the tree.
    async fn visit(&self, day: Day, tree: TreeId, found: &mut FoundTree) -> DayOutcome {
        let flushes = match self.manifests.day(day).await {
            Ok(Some(flushes)) => flushes,
            Ok(None) => return DayOutcome::Unknown,
            Err(error) => {
                found.problems.push(error.into());
                return DayOutcome::Unknown;
            }
        };
        let candidates: Vec<(ObjectName, FileStats)> = flushes
            .holding(tree)
            .map(|group| (group.states_file(), group.states()))
            .collect();
        if candidates.is_empty() {
            return DayOutcome::Negative;
        }
        for read in self.read(candidates, tree).await {
            match read {
                Ok(rows) => found.rows.extend(rows),
                Err(error) => found.problems.push(error.into()),
            }
        }
        DayOutcome::Positive
    }

    /// The rows of `tree` in each of `candidates`, in their order.
    async fn read(
        &self,
        candidates: Vec<(ObjectName, FileStats)>,
        tree: TreeId,
    ) -> Vec<Result<Vec<StateRow>, TreeReadError>> {
        futures::stream::iter(candidates)
            .map(|(file, stats)| async move { self.files.tree(&file, stats, tree).await })
            .buffered(self.config.concurrent_files.get())
            .collect()
            .await
    }
}
