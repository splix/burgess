//! The keys that one flush adds to the filters of its day.

use std::collections::HashSet;

use burgess_core::storage::{RefsKey, TreeKey};
use burgess_core::wire::UnixMillis;

use crate::batch::SortedBatch;

/// The keys of one flush, each one time.
///
/// A filter counts each key that it is given, and the counts of a day size
/// the filters of the next days. Thus a key that the batch holds many times
/// is given once. Keys are compared as bytes: references that differ only
/// in the case of ASCII letters make one key.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct FlushKeys {
    /// The references of the day and of the refs file of the flush.
    pub(super) refs: HashSet<RefsKey>,
    /// The trees in the states file of the flush.
    pub(super) trees: HashSet<TreeKey>,
}

impl FlushKeys {
    /// The keys of `batch`, stored by the flush `nnnn`.
    pub(super) fn of(batch: &SortedBatch, nnnn: UnixMillis) -> Self {
        let mut refs = HashSet::new();
        for row in &batch.refs {
            refs.insert(RefsKey::day(&row.ref_type, &row.domain_id));
            refs.insert(RefsKey::file(nnnn, &row.ref_type, &row.domain_id));
        }
        let trees = batch
            .states
            .iter()
            .map(|row| TreeKey::file(nnnn, row.tree))
            .collect();
        FlushKeys { refs, trees }
    }
}
