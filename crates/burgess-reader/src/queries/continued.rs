//! A request that continues an answer with a cursor.

use burgess_core::storage::Day;

use crate::cursor::{Cursor, Cursors, Position, QueryHash, RowPosition, Window};

use super::error::RequestError;
use super::{Queries, non_empty};

/// Where the page of a request starts: on the first page, or after the
/// cursor of the page before.
pub(super) struct Continuation {
    query: QueryHash,
    /// The window of the first page, which all pages of the answer use.
    pub(super) window: Window,
    cursor: Option<Cursor>,
}

impl Continuation {
    /// The last row given, if the page continues an answer.
    pub(super) fn position(&self) -> Option<Position> {
        self.cursor.map(|cursor| cursor.position)
    }

    /// The last row given, if the page continues an answer without an
    /// order. Refused for a cursor of an answer by time.
    pub(super) fn unordered(&self) -> Result<Option<RowPosition>, RequestError> {
        match self.position() {
            None => Ok(None),
            Some(Position::Unordered(row)) => Ok(Some(row)),
            Some(Position::Ordered { .. }) => Err(RequestError::CursorMismatch),
        }
    }
}

impl Queries {
    /// Where the page of the request with the hash `query` starts. With
    /// the cursor `text`, it must be given for this request, and its row
    /// must still be stored. Without it, the answer starts with the window
    /// that `first_window` makes.
    pub(super) async fn continuation(
        &self,
        query: QueryHash,
        text: Option<&str>,
        first_window: impl FnOnce() -> Result<Window, RequestError>,
    ) -> Result<Continuation, RequestError> {
        let Some(text) = non_empty(text) else {
            return Ok(Continuation {
                query,
                window: first_window()?,
                cursor: None,
            });
        };
        let cursor = Cursor::decode(text, query)?;
        self.check_stored(cursor.position.row()).await?;
        Ok(Continuation {
            query,
            window: cursor.window,
            cursor: Some(cursor),
        })
    }

    /// The cursors of the page that `continuation` started, after a walk
    /// whose oldest day is `oldest`, or `None` for a walk oldest day first.
    ///
    /// Refused when retention deleted the oldest day that the rest of the
    /// answer reads, since the page before: the walk read the day as one
    /// without rows. The check is after the walk, since retention can move
    /// while it reads.
    pub(super) fn cursors(
        &self,
        continuation: &Continuation,
        oldest: Option<Day>,
        by_time: bool,
    ) -> Result<Cursors, RequestError> {
        if let Some(cursor) = continuation.cursor
            && self
                .manifests
                .oldest_kept()
                .is_some_and(|kept| kept > cursor.oldest)
        {
            return Err(RequestError::Expired);
        }
        Ok(Cursors {
            query: continuation.query,
            window: continuation.window,
            by_time,
            oldest,
        })
    }

    /// Refuses a cursor whose file is gone. A walk would read the day as
    /// one without rows, and the answer would end with no word that rows
    /// are missing.
    async fn check_stored(&self, row: RowPosition) -> Result<(), RequestError> {
        match self.manifests.day(row.day).await {
            Ok(Some(flushes))
                if flushes
                    .groups()
                    .iter()
                    .any(|group| group.nnnn() == row.nnnn) =>
            {
                Ok(())
            }
            Ok(_) => Err(RequestError::Expired),
            // The walk reads the manifest again, and its problem then tells
            // which rows can be missing.
            Err(_) => Ok(()),
        }
    }
}
