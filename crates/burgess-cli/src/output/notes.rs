//! What may be missing from a page.

use std::io::{self, Write};

use burgess_api::Page;

/// Says on `err` why rows may be missing: each problem the service met, and
/// whether the page was cut at its limit.
pub fn write_notes(page: &Page, err: &mut dyn Write) -> io::Result<()> {
    for problem in &page.problems {
        writeln!(err, "problem: {problem}")?;
    }
    if !page.problems.is_empty() {
        writeln!(
            err,
            "the service could not read everything; rows or payloads may be missing"
        )?;
    }
    if page.truncated {
        writeln!(
            err,
            "more rows exist than the {} shown; a larger --limit, up to the service's maximum, shows more",
            page.rows.len()
        )?;
    }
    Ok(())
}
