//! Looking states up in the order of their time.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{FileStats, ObjectName};
use burgess_core::wire::UnixMillis;
use futures::StreamExt;
use futures::stream::FuturesOrdered;

use super::{FoundStates, StatesLookup, StatesLookupError};
use crate::day_scan::{DayScan, Found};
use crate::manifests::FlushGroup;
use crate::ordering::{Smallest, TsOrder};
use crate::states_file::{StatesFilter, StoredState};

/// A states file that may hold states of the window.
struct Candidate {
    file: ObjectName,
    stats: FileStats,
    /// The key in the order of the first state that the file can hold.
    bound: i64,
    nnnn: UnixMillis,
}

/// The keys of the heap: the time in the order, the rank of the file, and
/// the position in what the file gives.
type Key = (i64, usize, usize);

impl StatesLookup {
    /// The first `wanted` states that pass `filter` in `order`.
    ///
    /// The files are sorted by tree, thus only the time bounds of the
    /// manifest tell which files can hold the first states. A bound is a
    /// limit on each state of its file, thus when no state of a file can be
    /// better than the worst state kept, the files after it are not read.
    ///
    /// States of the same time are in the order of the files, then in their
    /// order in a file, thus the same request gives the same order.
    pub(super) async fn ordered(
        &self,
        filter: Arc<StatesFilter>,
        wanted: NonZeroUsize,
        order: TsOrder,
    ) -> FoundStates {
        let scan = DayScan {
            manifests: &self.manifests,
            config: self.config,
        };
        let pick = |group: &FlushGroup| candidate(group, &filter, order);
        let (mut candidates, mut problems): (Vec<Candidate>, Vec<StatesLookupError>) =
            scan.picked(self.days(&filter), pick).await;
        candidates.sort_by_key(|candidate| (candidate.bound, candidate.nnnn));
        let read = |rank: usize, candidate: Candidate, filter: Arc<StatesFilter>| async move {
            let read = self
                .files
                .states(
                    &candidate.file,
                    candidate.stats,
                    &filter,
                    wanted,
                    Some(order),
                )
                .await;
            (rank, candidate.bound, read)
        };
        let mut candidates = candidates.into_iter().enumerate().peekable();
        // Reads finish in the order they start, thus the states of the same
        // time come in the order of the files.
        let mut reads = FuturesOrdered::new();
        let mut kept: Smallest<Key, StoredState> = Smallest::new(wanted);
        loop {
            while reads.len() < self.config.concurrent_files.get()
                && let Some((rank, candidate)) =
                    candidates.next_if(|(_, candidate)| may_improve(&kept, candidate.bound))
            {
                let filter = narrowed(&filter, &kept, order);
                reads.push_back(read(rank, candidate, filter));
            }
            let Some((rank, bound, read)) = reads.next().await else {
                break;
            };
            // The heap can fill while the file is read.
            if !may_improve(&kept, bound) {
                continue;
            }
            match read {
                Ok(states) => {
                    for (position, state) in states.into_iter().enumerate() {
                        kept.offer((order.key(state.time), rank, position), state);
                    }
                }
                Err(error) => problems.push(error.into()),
            }
        }
        Found {
            rows: kept.into_sorted(),
            problems,
        }
    }
}

/// Whether a file whose states have keys of at least `bound` can hold a
/// state that the heap keeps. A state of the same key as the worst kept
/// comes after it, since it is in a later file.
fn may_improve(kept: &Smallest<Key, StoredState>, bound: i64) -> bool {
    kept.bar().is_none_or(|&(bar, _, _)| bound < bar)
}

/// `filter`, with its window cut at the time of the worst state kept when
/// the heap is full. A state after it cannot be kept, thus the row filter
/// drops it before its payload is read.
fn narrowed(
    filter: &Arc<StatesFilter>,
    kept: &Smallest<Key, StoredState>,
    order: TsOrder,
) -> Arc<StatesFilter> {
    let Some(&(bar, _, _)) = kept.bar() else {
        return Arc::clone(filter);
    };
    let worst = order.time(bar);
    let mut narrowed = StatesFilter::clone(filter);
    match order {
        TsOrder::Ascending => narrowed.until = narrowed.until.min(worst),
        TsOrder::Descending => narrowed.since = narrowed.since.max(worst),
    }
    Arc::new(narrowed)
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
        file: group.states_file(),
        stats: group.states(),
        bound: order.key(first),
        nnnn: group.nnnn(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use burgess_core::flow::{Subflow, TreeId};
    use burgess_core::wire::PayloadType;
    use jiff::Timestamp;

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
    fn kept_at(order: TsOrder, size: usize, times: &[u64]) -> Smallest<Key, StoredState> {
        let mut kept = Smallest::new(NonZeroUsize::new(size).unwrap());
        for (position, &time) in times.iter().enumerate() {
            let state = StoredState {
                tree: TreeId::from([1; 12]),
                subflow: Subflow::ROOT,
                time: UnixMillis::from(time),
                service: "checkout".into(),
                category: "step".into(),
                meta: BTreeMap::new(),
                payload_type: PayloadType::Json,
                payload: b"{}".to_vec(),
            };
            kept.offer((order.key(state.time), 0, position), state);
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
