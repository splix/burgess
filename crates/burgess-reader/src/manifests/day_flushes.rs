//! The flushes of one day that readers may use.

use burgess_core::flow::TreeId;
use burgess_core::storage::{Day, DayManifest, FileStats, FlushEntry, ObjectName, StatesBounds};
use burgess_core::wire::UnixMillis;
use jiff::Timestamp;

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

    /// Whether the states file may hold rows of `tree`, by its bounds alone.
    /// A file without rows has no bounds and holds nothing.
    pub fn may_hold(&self, tree: TreeId) -> bool {
        self.entry
            .states_bounds
            .is_some_and(|bounds| bounds.min_tree <= tree && tree <= bounds.max_tree)
    }

    /// Whether the states file may hold rows timed from `since` to `until`,
    /// by its bounds alone. A file without rows has no bounds and holds
    /// nothing.
    pub fn may_hold_times(&self, since: Timestamp, until: Timestamp) -> bool {
        self.entry.states_bounds.is_some_and(|bounds| {
            bounds.min_ts.saturating_timestamp() <= until
                && since <= bounds.max_ts.saturating_timestamp()
        })
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

    /// The flushes whose states file may hold rows of `tree`, oldest first.
    /// None of them means the day is negative for the tree: the bounds are
    /// exact at the day level, so no file of the day needs opening.
    pub fn holding(&self, tree: TreeId) -> impl Iterator<Item = &FlushGroup> {
        self.groups.iter().filter(move |group| group.may_hold(tree))
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

#[cfg(test)]
mod tests {
    use burgess_core::wire::UnixMillis;

    use super::*;

    fn entry(nnnn: u64, trees: Option<(u8, u8)>) -> FlushEntry {
        FlushEntry {
            nnnn: UnixMillis::from(nnnn),
            seq: nnnn,
            states_bounds: trees.map(|(min, max)| StatesBounds {
                min_tree: TreeId::from([min; 12]),
                max_tree: TreeId::from([max; 12]),
                min_ts: UnixMillis::from(0),
                max_ts: UnixMillis::from(0),
            }),
            states: FileStats {
                rows: u64::from(trees.is_some()),
                bytes: 100,
            },
            refs: FileStats::default(),
        }
    }

    fn flushes(entries: Vec<FlushEntry>) -> DayFlushes {
        DayFlushes::from(DayManifest {
            entries,
            ..DayManifest::new("20261107".parse().unwrap(), "writer-1".parse().unwrap())
        })
    }

    fn holding(flushes: &DayFlushes, tree: u8) -> Vec<u64> {
        flushes
            .holding(TreeId::from([tree; 12]))
            .map(|group| u64::from(group.nnnn()))
            .collect()
    }

    #[test]
    fn keeps_the_flushes_whose_bounds_include_the_tree() {
        let day = flushes(vec![
            entry(1, Some((1, 3))),
            entry(2, Some((4, 6))),
            entry(3, Some((3, 5))),
        ]);
        assert_eq!(holding(&day, 3), [1, 3]);
        assert_eq!(holding(&day, 6), [2]);
    }

    #[test]
    fn bounds_are_inclusive() {
        let day = flushes(vec![entry(1, Some((2, 4)))]);
        assert_eq!(holding(&day, 2), [1]);
        assert_eq!(holding(&day, 4), [1]);
        assert_eq!(holding(&day, 5), [] as [u64; 0]);
    }

    #[test]
    fn a_flush_without_states_holds_nothing() {
        let day = flushes(vec![entry(1, None)]);
        assert_eq!(holding(&day, 2), [] as [u64; 0]);
    }

    /// A flush whose states are timed from `min` to `max` milliseconds.
    fn timed(min: u64, max: u64) -> FlushGroup {
        let mut entry = entry(1, Some((1, 1)));
        let bounds = entry.states_bounds.as_mut().unwrap();
        bounds.min_ts = UnixMillis::from(min);
        bounds.max_ts = UnixMillis::from(max);
        flushes(vec![entry]).groups()[0].clone()
    }

    fn ms(ms: i64) -> Timestamp {
        Timestamp::from_millisecond(ms).unwrap()
    }

    #[test]
    fn time_bounds_are_inclusive_at_both_ends() {
        let group = timed(100, 200);
        assert!(group.may_hold_times(ms(200), ms(300)));
        assert!(group.may_hold_times(ms(0), ms(100)));
        assert!(group.may_hold_times(ms(120), ms(150)));
        assert!(group.may_hold_times(ms(0), ms(300)));
        assert!(!group.may_hold_times(ms(201), ms(300)));
        assert!(!group.may_hold_times(ms(0), ms(99)));
    }

    #[test]
    fn a_flush_without_states_holds_no_time() {
        let group = flushes(vec![entry(1, None)]).groups()[0].clone();
        assert!(!group.may_hold_times(ms(0), ms(i64::from(u32::MAX))));
    }
}
