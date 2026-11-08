//! Printing a page of rows.
//!
//! Rows go to standard output and nothing else does, so that JSON lines can
//! be piped as they are. What may be missing from them, problems and a cut
//! page, goes to standard error in either form.

mod compact;
mod json_lines;
mod listing;
mod notes;

use std::io::{self, Write};

use burgess_api::{Page, RefRow};
use clap::ValueEnum;

pub use listing::{Heading, RefsHeading, StatesHeading};
pub use notes::write_notes;

/// How rows are printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Output {
    /// One aligned line per row, to read.
    Listing,
    /// One row per line, as the API returns it.
    #[value(name = "jsonl")]
    JsonLines,
}

impl Output {
    /// Prints the rows of `page`, of the tree or the flow `heading` names.
    pub fn write_rows(self, heading: Heading, page: &Page, out: &mut dyn Write) -> io::Result<()> {
        match self {
            Output::Listing => listing::write(heading, page, out),
            Output::JsonLines => json_lines::write(page, out),
        }
    }

    /// Prints the states of `page`, of the service and category `heading`
    /// names.
    pub fn write_states(
        self,
        heading: &StatesHeading,
        page: &Page,
        out: &mut dyn Write,
    ) -> io::Result<()> {
        match self {
            Output::Listing => listing::write_states(heading, page, out),
            Output::JsonLines => json_lines::write(page, out),
        }
    }

    /// Prints the references of `page`, to the id `heading` names.
    pub fn write_refs(
        self,
        heading: &RefsHeading,
        page: &Page<RefRow>,
        out: &mut dyn Write,
    ) -> io::Result<()> {
        match self {
            Output::Listing => listing::write_refs(heading, page, out),
            Output::JsonLines => json_lines::write(page, out),
        }
    }
}

#[cfg(test)]
mod tests;
