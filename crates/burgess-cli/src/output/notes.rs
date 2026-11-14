//! What may be missing from a page.

use std::io::{self, Write};

use burgess_api::Page;

/// Says on `err` why rows may be missing: each problem the service met, and
/// whether more rows come after the page, with the cursor to print them.
pub fn write_notes<R>(page: &Page<R>, err: &mut dyn Write) -> io::Result<()> {
    for problem in &page.problems {
        writeln!(err, "problem: {problem}")?;
    }
    if !page.problems.is_empty() {
        writeln!(
            err,
            "the service could not read everything; rows or payloads may be missing"
        )?;
    }
    if let Some(cursor) = &page.cursor {
        // The cursor is last, thus it is easy to copy.
        writeln!(
            err,
            "more rows exist after the {} shown; to print them, repeat the command with --cursor {cursor}",
            page.rows.len()
        )?;
    }
    Ok(())
}
