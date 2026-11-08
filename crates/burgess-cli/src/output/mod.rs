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

use burgess_api::Page;
use clap::ValueEnum;

pub use listing::Heading;
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
    /// Prints the rows of `page`, found by the lookup `heading` names.
    pub fn write_rows(self, heading: Heading, page: &Page, out: &mut dyn Write) -> io::Result<()> {
        match self {
            Output::Listing => listing::write(heading, page, out),
            Output::JsonLines => json_lines::write(page, out),
        }
    }
}

#[cfg(test)]
mod tests;
