//! Rows as JSON lines.

use std::io::{self, Write};

use burgess_api::Page;

use super::compact::compact;

/// One row per line, in the API's own shape, so tools reading the API read
/// these lines too.
pub(super) fn write(page: &Page, out: &mut dyn Write) -> io::Result<()> {
    for row in &page.rows {
        let line = serde_json::to_string(row).map_err(io::Error::other)?;
        writeln!(out, "{}", compact(&line))?;
    }
    Ok(())
}
