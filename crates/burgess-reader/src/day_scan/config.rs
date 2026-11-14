//! How a walk over days reads.

use std::num::NonZeroUsize;

use crate::tree_lookup::TreeLookupConfig;

/// Settings of a walk over days, for a refs lookup or a states lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanConfig {
    /// Days whose filters and manifests are read ahead of the files. Each
    /// read waits on a round trip to the store, and most days of a walk have
    /// no row that the lookup wants. Each day read ahead holds its filter,
    /// at least a megabyte, in memory.
    pub concurrent_days: NonZeroUsize,
    /// Files read at the same time, across days.
    pub concurrent_files: NonZeroUsize,
    /// Days before the day of `since` that a lookup also reads. A producer
    /// clock ahead of the writer clock across midnight stores a row on the
    /// day before its own time.
    pub days_before: u32,
}

impl Default for ScanConfig {
    /// Four days ahead, provisional until lookups are measured on real data.
    /// The files and the days before `since` are the same as for a tree
    /// lookup, because the two settings apply to all lookups.
    fn default() -> Self {
        let tree = TreeLookupConfig::default();
        ScanConfig {
            concurrent_days: NonZeroUsize::new(4).expect("not zero"),
            concurrent_files: tree.concurrent_files,
            days_before: tree.days_before,
        }
    }
}
