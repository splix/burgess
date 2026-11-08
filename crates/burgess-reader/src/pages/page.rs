//! A lookup's rows and problems as one page.

use std::num::NonZeroUsize;

use burgess_api::Page;
use burgess_core::report::error_chain;

use crate::tree_lookup::FoundTree;

impl FoundTree {
    /// The first `limit` rows found, and every problem met on the way.
    ///
    /// There is no cursor to ask for the next page yet, so the lookup has
    /// already read every row by the time it is cut, and the page only says
    /// whether it was.
    pub fn into_page(self, limit: NonZeroUsize) -> Page {
        let truncated = self.rows.len() > limit.get();
        let mut problems: Vec<String> = self
            .problems
            .iter()
            .map(|problem| error_chain(problem))
            .collect();
        let rows = self
            .rows
            .into_iter()
            .take(limit.get())
            .map(|row| super::row::api_row(row, &mut problems))
            .collect();
        Page {
            rows,
            truncated,
            problems,
        }
    }
}
