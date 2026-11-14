//! How far a tree lookup walks, and how many files it reads at once.

use std::num::NonZeroUsize;

/// Settings of a [`TreeLookup`](super::TreeLookup).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeLookupConfig {
    /// Negative days in a row, from the id's own day on, a lookup without
    /// `until` walks past before it stops. A tree may skip a day, for one that runs past midnight while
    /// its producer goes quiet, so a single negative day is not proof that
    /// the tree ended.
    pub stop_slack_days: u32,
    /// Days before the id's own day a lookup starts at. A producer clock
    /// ahead of the writer's across midnight mints an id dated the day after
    /// the flush that stores its first rows.
    pub days_before: u32,
    /// Candidate files of one day read at the same time. Each read waits on
    /// round trips to the store, so reading them together saves latency.
    pub concurrent_files: NonZeroUsize,
    /// Candidate files of one day above which the lookup reads the day's
    /// trees filter, and opens only the files that it can hold the tree in.
    /// Fewer candidates are opened at once without it, thus the filter
    /// would only add a request. The settings make it `concurrent_files`
    /// when they do not give it.
    pub bloom_above_candidates: usize,
}

impl Default for TreeLookupConfig {
    /// One day of slack after the last positive day, no day before the id's
    /// own, four files at a time, and the filter above four candidates.
    ///
    /// The slack is decided. Starting before the id's day is undecided until
    /// clock skew is measured, so none is assumed. Four concurrent files is
    /// provisional: most trees live in one or two files a day. The filter
    /// threshold is provisional too: as many candidates as are read at once
    /// cost one wave of requests, which the filter would not shorten.
    fn default() -> Self {
        let concurrent_files = NonZeroUsize::new(4).expect("not zero");
        TreeLookupConfig {
            stop_slack_days: 1,
            days_before: 0,
            concurrent_files,
            bloom_above_candidates: concurrent_files.get(),
        }
    }
}
