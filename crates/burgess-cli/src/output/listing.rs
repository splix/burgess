//! Rows as a listing to read.

use std::fmt;
use std::io::{self, Write};

use burgess_api::{Page, Row, RowKind};
use burgess_core::flow::{FlowId, Subflow, TreeId};

use super::compact::compact;

/// What a listing is of, named in its first line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heading {
    /// A whole tree.
    Tree(TreeId),
    /// A flow and the flows below it.
    Flow(FlowId),
}

impl fmt::Display for Heading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Heading::Tree(tree) => write!(f, "tree {tree}"),
            Heading::Flow(flow) => write!(f, "flow {flow}"),
        }
    }
}

/// The heading, one aligned line per row, and a count.
///
/// Rows of one lookup share their tree, so a row names only its subflow,
/// as the last 8 hex characters of its flow id.
pub(super) fn write(heading: Heading, page: &Page, out: &mut dyn Write) -> io::Result<()> {
    writeln!(out, "{heading}")?;
    writeln!(out)?;
    let width = page
        .rows
        .iter()
        .map(|row| topic(row).chars().count())
        .max()
        .unwrap_or(0);
    for row in &page.rows {
        writeln!(
            out,
            "{:.3}  {}  {:<width$}  {:<5}  {}",
            row.ts,
            subflow(row.subflow),
            topic(row),
            kind(row.kind),
            detail(row),
        )?;
    }
    if !page.rows.is_empty() {
        writeln!(out)?;
    }
    writeln!(out, "{}", count(page.rows.len()))
}

fn topic(row: &Row) -> String {
    format!("{}/{}", printable(&row.service), printable(&row.category))
}

fn subflow(subflow: Subflow) -> String {
    format!("{:08x}", u32::from(subflow))
}

fn kind(kind: RowKind) -> &'static str {
    match kind {
        RowKind::State => "state",
        RowKind::Split => "split",
        RowKind::Unknown => "?",
    }
}

/// What the row says: a state's meta and payload, or the child a split
/// started.
fn detail(row: &Row) -> String {
    if let Some(child) = row.child_subflow {
        return format!("-> {}", subflow(child));
    }
    let mut parts: Vec<String> = row
        .meta
        .iter()
        .map(|(key, value)| format!("{}={}", printable(key), quoted(value)))
        .collect();
    // A state without a payload had one that could not be embedded, which
    // the problems explain.
    parts.push(
        row.payload
            .as_ref()
            .map_or_else(|| "-".to_string(), |payload| compact(payload.get())),
    );
    parts.join(" ")
}

/// `value` as is, or quoted, with control characters escaped, when it would
/// not read as one word.
fn quoted(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || c == '"')
    {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}

/// `text` with its control characters escaped.
///
/// Producers name services, categories and meta keys as they like, and a
/// listing goes to a terminal: an escape sequence could rewrite what the
/// investigator sees, and a line break would split a row.
fn printable(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_control() {
                c.escape_default().to_string()
            } else {
                c.to_string()
            }
        })
        .collect()
}

fn count(rows: usize) -> String {
    match rows {
        0 => "no rows".to_string(),
        1 => "1 row".to_string(),
        rows => format!("{rows} rows"),
    }
}
