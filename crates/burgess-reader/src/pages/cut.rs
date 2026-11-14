//! The rule that cuts a page, the same for each lookup.

use std::error::Error;
use std::num::NonZeroUsize;

use burgess_core::report::error_chain;

/// What a page shows of a lookup: its problems, and its first `limit`
/// rows, each with the cursor after it.
pub(super) struct Cut<T> {
    /// The problems, as text to show to a person.
    pub(super) problems: Vec<String>,
    /// The rows that the page holds, each with the cursor after it. The
    /// last row of the answer has no cursor, thus a client knows that it
    /// has all rows.
    pub(super) rows: Vec<(T, Option<String>)>,
}

impl<T> Cut<T> {
    /// The page of `rows`, which hold one row more than the page when the
    /// answer has more. `cursor` writes the cursor after a row.
    pub(super) fn new(
        problems: &[impl Error + 'static],
        rows: Vec<T>,
        limit: NonZeroUsize,
        cursor: impl Fn(&T) -> String,
    ) -> Self {
        let more = rows.len() > limit.get();
        let shown = rows.len().min(limit.get());
        let rows = rows
            .into_iter()
            .take(shown)
            .enumerate()
            .map(|(index, row)| {
                let cursor = (more || index + 1 < shown).then(|| cursor(&row));
                (row, cursor)
            })
            .collect();
        Cut {
            problems: problems
                .iter()
                .map(|problem| error_chain(problem))
                .collect(),
            rows,
        }
    }
}
