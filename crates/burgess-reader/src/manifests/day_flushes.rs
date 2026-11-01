//! The flushes of one day that readers may use.

use burgess_core::storage::{Day, DayManifest, FileStats, FlushEntry, ObjectName, StatesBounds};
use burgess_core::wire::UnixMillis;

/// One flush: a states file and a refs file written together.
///
/// Only flushes the manifest lists are groups. The writer adds a flush to the
/// manifest after both its files are stored, so a listed group is always
/// complete, and a file left by an unfinished flush is never seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushGroup {
    day: Day,
    entry: FlushEntry,
}

impl FlushGroup {
    /// The flush time by the writer clock, which names both files.
    pub fn nnnn(&self) -> UnixMillis {
        self.entry.nnnn
    }

    /// The writer's batch counter for this flush.
    pub fn seq(&self) -> u64 {
        self.entry.seq
    }

    /// Smallest and largest trees and times in the states file, or `None`
    /// when it has no rows, so a lookup can skip it unopened.
    pub fn states_bounds(&self) -> Option<StatesBounds> {
        self.entry.states_bounds
    }

    /// Rows and size of the states file.
    pub fn states(&self) -> FileStats {
        self.entry.states
    }

    /// Rows and size of the refs file.
    pub fn refs(&self) -> FileStats {
        self.entry.refs
    }

    /// Where the states file is stored.
    pub fn states_file(&self) -> ObjectName {
        ObjectName::StatesFile {
            day: self.day,
            nnnn: self.entry.nnnn,
        }
    }

    /// Where the refs file is stored.
    pub fn refs_file(&self) -> ObjectName {
        ObjectName::RefsFile {
            day: self.day,
            nnnn: self.entry.nnnn,
        }
    }
}

/// What a day's manifest says readers may open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayFlushes {
    day: Day,
    sealed: bool,
    groups: Vec<FlushGroup>,
}

impl DayFlushes {
    /// The day.
    pub fn day(&self) -> Day {
        self.day
    }

    /// Whether the day will never change again.
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// The day's flushes, oldest first.
    pub fn groups(&self) -> &[FlushGroup] {
        &self.groups
    }
}

impl From<DayManifest> for DayFlushes {
    fn from(manifest: DayManifest) -> Self {
        let day = manifest.day;
        DayFlushes {
            day,
            sealed: manifest.sealed,
            groups: manifest
                .entries
                .into_iter()
                .map(|entry| FlushGroup { day, entry })
                .collect(),
        }
    }
}
