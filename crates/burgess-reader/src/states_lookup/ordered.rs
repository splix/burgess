//! Looking states up in the order of their time.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{FileStats, ObjectName};
use futures::StreamExt;
use futures::stream::FuturesOrdered;

use super::scan::TimeKey;
use super::{FoundStates, StatesLookup, StatesLookupError};
use crate::cursor::{Given, Positioned, RowPosition, Walk};
use crate::day_scan::{DayScan, Found};
use crate::manifests::FlushGroup;
use crate::ordering::{Smallest, TsOrder};
use crate::states_file::{FileKey, ScanOrder, StatesFilter, StoredState};

/// A states file that may hold states of the window.
struct Candidate {
    group: FlushGroup,
    file: ObjectName,
    stats: FileStats,
    /// The smallest key that a state of the file can have.
    first: TimeKey,
}

/// The states kept, by their key in the order.
type Kept = Smallest<TimeKey, Positioned<StoredState>>;

impl StatesLookup {
    /// The first `wanted` states that pass `filter` in `order`, after the
    /// key `from` if given.
    ///
    /// The files are sorted by tree, thus only the time bounds of the
    /// manifest tell which files can hold the first states. A bound is a
    /// limit on each state of its file, thus when no state of a file can be
    /// better than the worst state kept, the files after it are not read.
    pub(super) async fn ordered(
        &self,
        filter: Arc<StatesFilter>,
        wanted: NonZeroUsize,
        order: TsOrder,
        from: Option<TimeKey>,
    ) -> FoundStates {
        let filter = match from {
            Some(from) => continued(&filter, order, from),
            None => filter,
        };
        let scan = DayScan {
            manifests: &self.manifests,
            config: self.config,
        };
        let pick = |group: &FlushGroup| candidate(group, &filter, order);
        let days = self.days(&filter);
        let oldest = days.last().copied();
        let (mut candidates, mut problems): (Vec<Candidate>, Vec<StatesLookupError>) =
            scan.picked(days, pick).await;
        candidates.sort_by_key(|candidate| candidate.first);
        let read = |candidate: Candidate, filter: Arc<StatesFilter>| async move {
            let after = from.and_then(|from| after_in(&candidate.group, from));
            let order = ScanOrder::ByTime { order, after };
            let read = self
                .files
                .states(&candidate.file, candidate.stats, &filter, wanted, order)
                .await;
            (candidate, read)
        };
        let mut candidates = candidates.into_iter().peekable();
        let mut reads = FuturesOrdered::new();
        let mut kept: Kept = Smallest::new(wanted);
        loop {
            while reads.len() < self.config.concurrent_files.get()
                && let Some(candidate) =
                    candidates.next_if(|candidate| may_improve(&kept, candidate.first))
            {
                let filter = narrowed(&filter, &kept, order);
                reads.push_back(read(candidate, filter));
            }
            let Some((candidate, read)) = reads.next().await else {
                break;
            };
            // The heap can fill while the file is read. Then no state of the
            // file is on this page, and neither is a problem of its read:
            // the next page reads the file again and reports it there.
            if !may_improve(&kept, candidate.first) {
                continue;
            }
            match read {
                Ok(states) => {
                    for state in states {
                        let state = Positioned::in_flush(&candidate.group, state);
                        let key = TimeKey {
                            time: order.key(state.row.time),
                            row: state.position,
                        };
                        kept.offer(key, state);
                    }
                }
                Err(error) => problems.push(error.into()),
            }
        }
        Found {
            rows: kept.into_sorted(),
            problems,
            oldest,
        }
    }
}

/// Whether a file whose states have keys of at least `first` can hold a
/// state that the heap keeps.
fn may_improve(kept: &Kept, first: TimeKey) -> bool {
    kept.bar().is_none_or(|&bar| first < bar)
}

/// `filter`, with its window cut at the time of the worst state kept when
/// the heap is full. A state after it cannot be kept, thus the row filter
/// drops it before its payload is read.
fn narrowed(filter: &Arc<StatesFilter>, kept: &Kept, order: TsOrder) -> Arc<StatesFilter> {
    let Some(bar) = kept.bar() else {
        return Arc::clone(filter);
    };
    let worst = order.time(bar.time);
    let mut narrowed = StatesFilter::clone(filter);
    match order {
        TsOrder::Ascending => narrowed.until = narrowed.until.min(worst),
        TsOrder::Descending => narrowed.since = narrowed.since.max(worst),
    }
    Arc::new(narrowed)
}

/// `filter`, with its window starting at the time of `from`, the last
/// state that a page gave. A state before it was on a page before.
fn continued(filter: &Arc<StatesFilter>, order: TsOrder, from: TimeKey) -> Arc<StatesFilter> {
    let last = order.time(from.time);
    let mut continued = StatesFilter::clone(filter);
    match order {
        TsOrder::Ascending => continued.since = continued.since.max(last),
        TsOrder::Descending => continued.until = continued.until.min(last),
    }
    Arc::new(continued)
}

/// The key in a file of `group` after which its states come after `from`.
///
/// States of the time of `from` come after it only in a later file, or
/// later in its own file. `None` when each state of the window comes after
/// it, as the window starts at that time.
fn after_in(group: &FlushGroup, from: TimeKey) -> Option<FileKey> {
    let row = match Given::of(group, from.row, Walk::OldestFirst) {
        Given::All => u64::MAX,
        Given::UpTo(index) => index,
        Given::Nothing => return None,
    };
    Some(FileKey {
        time: from.time,
        row,
    })
}

/// `group` as a candidate, if its states file may hold states of the
/// window of `filter`.
fn candidate(group: &FlushGroup, filter: &StatesFilter, order: TsOrder) -> Option<Candidate> {
    let bounds = group.states_bounds()?;
    if !group.may_hold_times(filter.since, filter.until) {
        return None;
    }
    let first = match order {
        TsOrder::Ascending => bounds.min_ts,
        TsOrder::Descending => bounds.max_ts,
    };
    Some(Candidate {
        group: group.clone(),
        file: group.states_file(),
        stats: group.states(),
        first: TimeKey {
            time: order.key(first),
            row: RowPosition::first_in(group),
        },
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use burgess_core::flow::{Subflow, TreeId};
    use burgess_core::wire::{PayloadType, UnixMillis};
    use jiff::Timestamp;

    use crate::test_support::days_ago;

    use super::*;

    fn at(ms: i64) -> Timestamp {
        Timestamp::from_millisecond(ms).unwrap()
    }

    fn window() -> Arc<StatesFilter> {
        Arc::new(StatesFilter {
            service: "checkout".into(),
            category: "step".into(),
            meta: BTreeMap::new(),
            since: at(1_000),
            until: at(9_000),
            payload: None,
        })
    }

    /// A heap of `size` that holds states at `times`.
    fn kept_at(order: TsOrder, size: usize, times: &[u64]) -> Kept {
        let mut kept = Smallest::new(NonZeroUsize::new(size).unwrap());
        for (index, &time) in times.iter().enumerate() {
            let position = RowPosition {
                day: days_ago(0),
                nnnn: UnixMillis::from(1),
                index: index as u64,
            };
            let row = StoredState {
                tree: TreeId::from([1; 12]),
                subflow: Subflow::ROOT,
                time: UnixMillis::from(time),
                service: "checkout".into(),
                category: "step".into(),
                meta: BTreeMap::new(),
                payload_type: PayloadType::Json,
                payload: b"{}".to_vec(),
            };
            let key = TimeKey {
                time: order.key(row.time),
                row: position,
            };
            kept.offer(key, Positioned { position, row });
        }
        kept
    }

    #[test]
    fn a_full_heap_cuts_the_window_at_its_worst_state() {
        let ascending = kept_at(TsOrder::Ascending, 2, &[2_000, 5_000]);
        let descending = kept_at(TsOrder::Descending, 2, &[8_000, 4_000]);

        let ascending = narrowed(&window(), &ascending, TsOrder::Ascending);
        let descending = narrowed(&window(), &descending, TsOrder::Descending);

        assert_eq!((ascending.since, ascending.until), (at(1_000), at(5_000)));
        assert_eq!((descending.since, descending.until), (at(4_000), at(9_000)));
    }

    #[test]
    fn a_heap_with_room_keeps_the_window() {
        let kept = kept_at(TsOrder::Ascending, 2, &[2_000]);

        let same = narrowed(&window(), &kept, TsOrder::Ascending);

        assert_eq!((same.since, same.until), (at(1_000), at(9_000)));
    }
}
