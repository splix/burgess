//! The problems of a walk that reads one row past its page.

use std::num::NonZeroUsize;

/// The problems that a walk meets, without those that belong to the next
/// page.
///
/// A walk that wants rows for a page reads one row more than the page
/// shows, and that row only tells that the answer goes on. A problem met
/// after the rows that the page shows can hide only later rows. When the
/// walk then finds the extra row, the problem is dropped: the next page
/// starts after the rows shown, meets the problem again and reports it.
/// Thus a client that joins the pages gets each problem one time.
pub(crate) struct WalkProblems<E> {
    /// The rows that the page shows, or `None` when the walk reads all.
    shown: Option<usize>,
    problems: Vec<E>,
    /// The problems met after the rows that the page shows.
    late: Vec<E>,
}

impl<E> WalkProblems<E> {
    /// The problems of a walk that stops at `wanted` rows, the rows of the
    /// page and the extra row, or that reads all rows without it.
    pub(crate) fn new(wanted: Option<NonZeroUsize>) -> Self {
        WalkProblems {
            shown: wanted.map(|wanted| wanted.get() - 1),
            problems: Vec::new(),
            late: Vec::new(),
        }
    }

    /// `problem`, met when the walk had found `found` rows.
    pub(crate) fn push(&mut self, problem: E, found: usize) {
        if self.shown.is_some_and(|shown| found >= shown) {
            self.late.push(problem);
        } else {
            self.problems.push(problem);
        }
    }

    /// The problems of the page, for a walk that found `found` rows.
    pub(crate) fn of_page(mut self, found: usize) -> Vec<E> {
        let cut = self.shown.is_some_and(|shown| found > shown);
        if !cut {
            self.problems.append(&mut self.late);
        }
        self.problems
    }
}
