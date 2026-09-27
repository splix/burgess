//! The bounds the manifest records for a batch's states file.

use burgess_core::storage::StatesBounds;

use crate::batch::SortedBatch;

impl SortedBatch {
    /// Bounds of the states rows, or `None` when there are none.
    ///
    /// Rows are sorted by tree first, so the tree bounds are the first and
    /// last rows; times are not sorted across trees and need a scan.
    pub(super) fn states_bounds(&self) -> Option<StatesBounds> {
        let first = self.states.first()?;
        let last = self.states.last()?;
        let times = self.states.iter().map(|row| row.time);
        Some(StatesBounds {
            min_tree: first.tree,
            max_tree: last.tree,
            min_ts: times.clone().min()?,
            max_ts: times.max()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use burgess_core::flow::{Subflow, TreeId};
    use burgess_core::wire::UnixMillis;

    use super::*;
    use crate::batch::{StateEntry, StateRow};

    fn row(tree: u8, ms: u64) -> StateRow {
        StateRow {
            tree: TreeId::from([tree; 12]),
            subflow: Subflow::ROOT,
            time: UnixMillis::from(ms),
            service: "s".into(),
            category: "c".into(),
            entry: StateEntry::Split {
                child: Subflow::from(1),
            },
        }
    }

    #[test]
    fn no_rows_no_bounds() {
        assert_eq!(SortedBatch::default().states_bounds(), None);
    }

    #[test]
    fn trees_from_the_ends_times_from_all_rows() {
        let batch = SortedBatch {
            states: vec![row(1, 50), row(1, 90), row(5, 10), row(9, 70)],
            ..SortedBatch::default()
        };
        assert_eq!(
            batch.states_bounds(),
            Some(StatesBounds {
                min_tree: TreeId::from([1; 12]),
                max_tree: TreeId::from([9; 12]),
                min_ts: UnixMillis::from(10),
                max_ts: UnixMillis::from(90),
            })
        );
    }
}
