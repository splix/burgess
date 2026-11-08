//! The rule that cuts a page, the same for each lookup.

use std::error::Error;
use std::num::NonZeroUsize;
use std::vec;

use burgess_core::report::error_chain;

/// What a page shows of a lookup: its problems, its first `limit` rows,
/// and whether more rows were found.
pub(super) struct Cut<T> {
    /// The problems, as text to show to a person.
    pub(super) problems: Vec<String>,
    /// The rows that the page holds.
    pub(super) rows: std::iter::Take<vec::IntoIter<T>>,
    /// Whether the lookup found more rows than the page holds.
    pub(super) truncated: bool,
}

impl<T> Cut<T> {
    pub(super) fn new(
        problems: &[impl Error + 'static],
        rows: Vec<T>,
        limit: NonZeroUsize,
    ) -> Self {
        Cut {
            problems: problems
                .iter()
                .map(|problem| error_chain(problem))
                .collect(),
            truncated: rows.len() > limit.get(),
            rows: rows.into_iter().take(limit.get()),
        }
    }
}
