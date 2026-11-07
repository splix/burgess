//! Which days a tree lookup visits, and when it stops.

use burgess_core::storage::Day;

/// What a visited day told about the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DayOutcome {
    /// The day has files, and none of them can hold the tree.
    Negative,
    /// Some file of the day may hold the tree.
    Positive,
    /// The day says nothing about the tree: the writer stored no file that
    /// day, or its manifest could not be read. A writer outage must not
    /// look like the end of the tree.
    Unknown,
}

/// Where a walk starts and where it may end.
#[derive(Debug, Clone, Copy)]
pub(super) struct WalkRange {
    /// The first day visited.
    pub(super) first: Day,
    /// The first day whose negative outcome counts towards stopping. Days
    /// before it are visited only in case a producer clock ran ahead, so
    /// finding nothing there says nothing about the tree.
    pub(super) counted_from: Day,
    /// The last day the writer can have stored anything on yet.
    pub(super) latest: Day,
}

/// The days of one lookup, from the first day forward.
#[derive(Debug)]
pub(super) struct DayWalk {
    next: Option<Day>,
    last: Day,
    counted_from: Day,
    stop: Stop,
}

/// When a walk ends before its last day.
#[derive(Debug)]
enum Stop {
    /// After this many counted negative days in a row.
    AfterNegatives { limit: u32, seen: u32 },
    /// Only at the last day.
    AtLastDay,
}

impl DayWalk {
    /// A walk over `range`. With `until` it ends the day after `until`,
    /// since a row is filed under the writer's flush day, which may come
    /// after the row's own time. Without it, it ends after `slack + 1`
    /// counted negative days in a row. It never passes `range.latest`.
    pub(super) fn new(range: WalkRange, until: Option<Day>, slack: u32) -> Self {
        let (last, stop) = match until {
            Some(until) => (
                until.next().unwrap_or(until).min(range.latest),
                Stop::AtLastDay,
            ),
            None => (
                range.latest,
                Stop::AfterNegatives {
                    limit: slack.saturating_add(1),
                    seen: 0,
                },
            ),
        };
        DayWalk {
            next: (range.first <= last).then_some(range.first),
            last,
            counted_from: range.counted_from,
            stop,
        }
    }

    /// The day to visit next, or `None` when the walk is over.
    pub(super) fn next_day(&self) -> Option<Day> {
        self.next
    }

    /// Records what the day returned by [`Self::next_day`] told, and moves on.
    pub(super) fn visited(&mut self, outcome: DayOutcome) {
        let Some(day) = self.next else {
            return;
        };
        let counted = day >= self.counted_from;
        let stopped = match (&mut self.stop, outcome) {
            (Stop::AfterNegatives { limit, seen }, DayOutcome::Negative) if counted => {
                *seen += 1;
                seen >= limit
            }
            (Stop::AfterNegatives { seen, .. }, DayOutcome::Positive) => {
                *seen = 0;
                false
            }
            _ => false,
        };
        self.next = if stopped || day >= self.last {
            None
        } else {
            day.next()
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day(n: u32) -> Day {
        format!("202611{n:02}").parse().unwrap()
    }

    fn range(first: u32, latest: u32) -> WalkRange {
        WalkRange {
            first: day(first),
            counted_from: day(first),
            latest: day(latest),
        }
    }

    /// The days visited when each day's outcome is looked up in the lists.
    fn visit(mut walk: DayWalk, positive: &[u32], unknown: &[u32]) -> Vec<u32> {
        let mut visited = Vec::new();
        while let Some(next) = walk.next_day() {
            let n = next.to_string()[6..].parse().unwrap();
            visited.push(n);
            walk.visited(if positive.contains(&n) {
                DayOutcome::Positive
            } else if unknown.contains(&n) {
                DayOutcome::Unknown
            } else {
                DayOutcome::Negative
            });
        }
        visited
    }

    #[test]
    fn stops_after_the_slack_past_the_first_negative_day() {
        let walk = DayWalk::new(range(1, 30), None, 1);
        assert_eq!(visit(walk, &[1, 2], &[]), [1, 2, 3, 4]);
    }

    #[test]
    fn a_positive_day_within_the_slack_resets_it() {
        let walk = DayWalk::new(range(1, 30), None, 1);
        assert_eq!(visit(walk, &[1, 3], &[]), [1, 2, 3, 4, 5]);
    }

    #[test]
    fn slack_is_configurable() {
        assert_eq!(
            visit(DayWalk::new(range(1, 30), None, 0), &[1], &[]),
            [1, 2]
        );
        assert_eq!(
            visit(DayWalk::new(range(1, 30), None, 2), &[1], &[]),
            [1, 2, 3, 4]
        );
    }

    #[test]
    fn unknown_days_neither_count_nor_reset() {
        let walk = DayWalk::new(range(1, 30), None, 1);
        assert_eq!(visit(walk, &[1, 5], &[3, 4]), [1, 2, 3, 4, 5, 6, 7]);
        let walk = DayWalk::new(range(1, 30), None, 1);
        assert_eq!(visit(walk, &[1], &[3]), [1, 2, 3, 4]);
    }

    #[test]
    fn days_before_the_counted_ones_do_not_stop_the_walk() {
        let early = WalkRange {
            first: day(1),
            counted_from: day(4),
            latest: day(30),
        };
        let walk = DayWalk::new(early, None, 1);
        assert_eq!(visit(walk, &[4], &[]), [1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn until_walks_past_negative_days_to_the_day_after() {
        let walk = DayWalk::new(range(1, 30), Some(day(6)), 1);
        assert_eq!(visit(walk, &[1], &[]), [1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn until_two_days_before_the_first_visits_nothing() {
        let walk = DayWalk::new(range(5, 30), Some(day(3)), 1);
        assert_eq!(visit(walk, &[], &[]), [] as [u32; 0]);
    }

    #[test]
    fn until_the_day_before_the_first_visits_the_first() {
        let walk = DayWalk::new(range(5, 30), Some(day(4)), 1);
        assert_eq!(visit(walk, &[], &[]), [5]);
    }

    #[test]
    fn never_passes_the_latest_day() {
        assert_eq!(
            visit(DayWalk::new(range(1, 3), Some(day(20)), 1), &[], &[]),
            [1, 2, 3]
        );
        assert_eq!(
            visit(DayWalk::new(range(1, 3), None, 1), &[1, 2, 3], &[]),
            [1, 2, 3]
        );
    }

    #[test]
    fn a_first_day_after_the_latest_visits_nothing() {
        let walk = DayWalk::new(range(9, 3), None, 1);
        assert_eq!(visit(walk, &[], &[]), [] as [u32; 0]);
    }
}
