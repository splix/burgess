//! The cursors that the rows of an answer carry.

use burgess_core::storage::Day;
use burgess_core::wire::UnixMillis;

use super::Cursor;
use super::position::{Position, RowPosition};
use super::query_hash::QueryHash;
use super::window::Window;

/// Writes the cursor after each row of an answer: the same request and
/// window, and the position of the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursors {
    /// The hash of the request.
    pub query: QueryHash,
    /// The window of the first page.
    pub window: Window,
    /// Whether the answer is ordered by time, thus its position needs the
    /// time of the row.
    pub by_time: bool,
    /// The oldest day that the walk of each page reads. `None` when the
    /// walk goes oldest day first, thus the rest of the answer starts on
    /// the day of the row.
    pub oldest: Option<Day>,
}

impl Cursors {
    /// The cursor after the row at `row`, whose time is `ts`.
    pub fn after(&self, row: RowPosition, ts: UnixMillis) -> String {
        let position = if self.by_time {
            Position::Ordered { ts, row }
        } else {
            Position::Unordered(row)
        };
        Cursor {
            window: self.window,
            position,
            oldest: self.oldest.unwrap_or(row.day),
        }
        .encode(self.query)
    }
}
