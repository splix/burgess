//! Narrowing a found tree down to one flow and the flows split off below it.

use std::collections::{HashMap, HashSet};

use burgess_core::flow::Subflow;
use burgess_core::storage::{StateEntry, StateRow};

use super::found::FoundTree;

impl FoundTree {
    /// The rows of `top` and of every flow split off below it, at any depth,
    /// in the order the tree holds them.
    ///
    /// The root keeps every row: each flow of a tree is below its root, even
    /// one no split leads to. A producer goes on with a child whose split it
    /// had to drop, and that child's rows still belong to the tree.
    ///
    /// Every problem of the tree is kept: a file that could not be read may
    /// have held rows of these flows, or a split leading to more of them.
    pub fn subtree(mut self, top: Subflow) -> FoundTree {
        if !top.is_root() {
            let flows = SplitLinks::of(self.rows.iter().map(|row| &row.row)).below(top);
            self.rows.retain(|row| flows.contains(&row.row.subflow));
        }
        self
    }
}

/// The children each flow of a tree split off, as its split rows name them.
struct SplitLinks(HashMap<Subflow, Vec<Subflow>>);

impl SplitLinks {
    fn of<'a>(rows: impl Iterator<Item = &'a StateRow>) -> Self {
        let mut links: HashMap<Subflow, Vec<Subflow>> = HashMap::new();
        for row in rows {
            if let StateEntry::Split { child } = &row.entry {
                links.entry(row.subflow).or_default().push(*child);
            }
        }
        SplitLinks(links)
    }

    /// `top` and every flow its splits lead to, directly or through other
    /// flows.
    ///
    /// Splits are stored as producers sent them, so one can lead back to a
    /// flow above it, or to itself. Each flow is visited once, which ends
    /// such a loop instead of hanging on it. A split to the root is not
    /// followed: the root is no flow's child, and following it would turn
    /// the lookup of any flow into a lookup of the whole tree.
    fn below(&self, top: Subflow) -> HashSet<Subflow> {
        let mut found = HashSet::from([top]);
        let mut pending = vec![top];
        while let Some(parent) = pending.pop() {
            for &child in self.0.get(&parent).into_iter().flatten() {
                if !child.is_root() && found.insert(child) {
                    pending.push(child);
                }
            }
        }
        found
    }
}

#[cfg(test)]
mod tests {
    use burgess_core::flow::TreeId;

    use super::*;
    use crate::test_support::{in_one_file, state_row, stored, subflows};

    /// Builds the rows of one tree in the order they are added, each a
    /// millisecond after the one before.
    #[derive(Default)]
    struct Rows(Vec<StateRow>);

    impl Rows {
        fn state(self, subflow: u32) -> Self {
            self.push(subflow, None)
        }

        fn split(self, parent: u32, child: u32) -> Self {
            self.push(parent, Some(child))
        }

        fn push(mut self, subflow: u32, child: Option<u32>) -> Self {
            let time = self.0.len() as u64;
            self.0
                .push(state_row(TreeId::from([1; 12]), subflow, time, child));
            self
        }

        fn found(self) -> FoundTree {
            FoundTree {
                rows: in_one_file(self.0),
                ..FoundTree::default()
            }
        }
    }

    fn subtree(rows: Rows, top: u32) -> Vec<u32> {
        subflows(&stored(&rows.found().subtree(Subflow::from(top)).rows))
    }

    /// The root splits off 1 and 4, 1 splits off 2, and 2 splits off 3.
    fn family() -> Rows {
        Rows::default()
            .state(0)
            .split(0, 1)
            .state(1)
            .split(1, 2)
            .split(0, 4)
            .state(2)
            .state(4)
            .split(2, 3)
            .state(3)
    }

    #[test]
    fn keeps_the_flow_and_every_flow_below_it() {
        assert_eq!(subtree(family(), 1), [1, 1, 2, 2, 3]);
    }

    #[test]
    fn leaves_out_siblings_and_the_flows_above() {
        let flows = subtree(family(), 2);

        assert!(!flows.contains(&4), "sibling of the parent: {flows:?}");
        assert!(!flows.contains(&1) && !flows.contains(&0), "{flows:?}");
    }

    #[test]
    fn a_leaf_flow_is_only_its_own_rows() {
        assert_eq!(subtree(family(), 4), [4]);
    }

    #[test]
    fn the_root_keeps_every_row_of_the_tree() {
        assert_eq!(subtree(family(), 0), subflows(&family().0));
    }

    #[test]
    fn the_root_keeps_a_flow_whose_split_was_lost() {
        let rows = Rows::default().state(0).state(6).split(0, 1).state(1);

        assert_eq!(subtree(rows, 0), [0, 6, 0, 1]);
    }

    #[test]
    fn an_unknown_flow_is_empty() {
        assert_eq!(subtree(family(), 9), Vec::<u32>::new());
    }

    #[test]
    fn follows_splits_filed_before_the_flows_own_rows() {
        let rows = Rows::default().split(2, 3).state(3).split(1, 2).state(1);

        assert_eq!(subtree(rows, 1), [2, 3, 1, 1]);
    }

    #[test]
    fn splits_that_loop_back_end_the_walk() {
        let rows = Rows::default()
            .split(1, 2)
            .split(2, 1)
            .split(2, 2)
            .state(2)
            .state(5);

        assert_eq!(subtree(rows, 1), [1, 2, 2, 2]);
    }

    #[test]
    fn a_split_to_the_root_is_not_followed() {
        let rows = Rows::default().state(0).split(1, 0).state(1);

        assert_eq!(subtree(rows, 1), [1, 1]);
    }
}
