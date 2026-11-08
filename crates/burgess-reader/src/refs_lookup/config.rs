//! How a refs lookup walks the days.

use std::num::NonZeroUsize;

use crate::tree_lookup::TreeLookupConfig;

/// Settings of a [`RefsLookup`](super::RefsLookup).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefsLookupConfig {
    /// Days whose manifests are read ahead of the files. Each read waits on
    /// a round trip to the store, and most days of a walk have no reference
    /// to the id.
    pub concurrent_days: NonZeroUsize,
    /// Refs files read at the same time, across days.
    pub concurrent_files: NonZeroUsize,
    /// Days before the day of `since` that a lookup also reads. A producer
    /// clock ahead of the writer clock across midnight stores a reference on
    /// the day before its own time.
    pub days_before: u32,
}

impl Default for RefsLookupConfig {
    /// Four days ahead, provisional until lookups are measured on real data.
    /// The files and the days before `since` are the same as for a tree
    /// lookup, because the two settings apply to the two lookups.
    fn default() -> Self {
        let tree = TreeLookupConfig::default();
        RefsLookupConfig {
            concurrent_days: NonZeroUsize::new(4).expect("not zero"),
            concurrent_files: tree.concurrent_files,
            days_before: tree.days_before,
        }
    }
}
