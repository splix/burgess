//! Asking for the pages of an answer until the CLI has the rows asked for.

use std::collections::HashSet;
use std::num::NonZeroU32;

use burgess_api::Page;

use crate::args::PagingArgs;
use crate::client::{ClientError, Reply};
use crate::requests::PagedRequest;

/// How many rows to print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Total {
    /// The rows of one page, as many as the service gives.
    OnePage,
    /// At most this many rows.
    Rows(NonZeroU32),
    /// All rows of the answer.
    All,
}

/// How many rows to print, how many to ask for at a time, and where in the
/// answer to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paging {
    /// How many rows to print.
    pub total: Total,
    /// The most rows that one request asks for, or `None` for the rows
    /// still to print, or for the service's default.
    pub page_size: Option<NonZeroU32>,
    /// The cursor to start after, from an earlier answer.
    pub cursor: Option<String>,
}

impl From<PagingArgs> for Paging {
    fn from(args: PagingArgs) -> Self {
        let total = match (args.limit, args.all) {
            (Some(limit), _) => Total::Rows(limit),
            (None, true) => Total::All,
            (None, false) => Total::OnePage,
        };
        Paging {
            total,
            page_size: args.page_size,
            cursor: args.cursor,
        }
    }
}

/// What the CLI does after the pages that it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    /// Ask for a page of at most `limit` rows, or of the service's default
    /// without one.
    Ask { limit: Option<u32> },
    /// Print the rows: the CLI has those asked for.
    Done,
}

impl Paging {
    /// What to do after `pages` pages that gave `rows` rows.
    fn next(&self, pages: usize, rows: usize) -> Next {
        let still = match self.total {
            Total::OnePage if pages > 0 => return Next::Done,
            Total::OnePage | Total::All => None,
            Total::Rows(total) => {
                let rows = u32::try_from(rows).unwrap_or(u32::MAX);
                match total.get().checked_sub(rows) {
                    Some(still) if still > 0 => Some(still),
                    _ => return Next::Done,
                }
            }
        };
        let page_size = self.page_size.map(NonZeroU32::get);
        let limit = match (still, page_size) {
            (Some(still), Some(page_size)) => Some(still.min(page_size)),
            (still, page_size) => still.or(page_size),
        };
        Next::Ask { limit }
    }
}

/// The rows of the pages that `ask` gets for `request`, as one page, up to
/// the rows that `paging` asks for. Its cursor continues after its last
/// row, and is `None` when the answer has no more rows, or cannot go on.
///
/// The service tells a problem on each page whose rows it can hide, thus a
/// problem of an earlier page is not told again. When a page after the
/// first cannot be read, the rows before it are kept, and a problem tells
/// why the answer stops there.
pub async fn follow<Q: PagedRequest, R>(
    request: Q,
    paging: &Paging,
    mut ask: impl AsyncFnMut(Q) -> Result<Reply<R>, ClientError>,
) -> Result<Page<R>, ClientError> {
    let mut joined = Page {
        rows: Vec::new(),
        cursor: paging.cursor.clone(),
        problems: Vec::new(),
    };
    let mut told = HashSet::new();
    let mut pages = 0;
    while let Next::Ask { limit } = paging.next(pages, joined.rows.len()) {
        let sent = joined.cursor.clone();
        let reply = match ask(request.clone().page(limit, sent.clone())).await {
            Ok(reply) => reply,
            Err(error) if pages > 0 => {
                stopped(&mut joined, &error);
                break;
            }
            Err(error) => return Err(error),
        };
        pages += 1;
        let page = reply.page;
        joined.rows.extend(page.rows);
        let new: Vec<String> = page
            .problems
            .into_iter()
            .filter(|problem| !told.contains(problem))
            .collect();
        told.extend(new.iter().cloned());
        joined.problems.extend(new);
        joined.cursor = page.cursor;
        // The cursor of a broken stream is printed, not followed: the
        // stream can break again.
        if reply.cut_short || joined.cursor.is_none() {
            break;
        }
        if joined.cursor == sent {
            joined.problems.push(
                "the service gave the cursor that it was sent, thus the answer stops here".into(),
            );
            joined.cursor = None;
            break;
        }
    }
    Ok(joined)
}

/// Notes in `joined` that the next page could not be read for `error`.
fn stopped<R>(joined: &mut Page<R>, error: &ClientError) {
    joined.problems.push(error.stopped_answer());
    // The service refuses the cursor again, thus it continues nothing.
    if error.is_usage() {
        joined.cursor = None;
    }
}
