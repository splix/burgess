//! Cursors that the service refuses.

use burgess_core::storage::DayError;

use super::window::WindowError;

/// Why a cursor cannot continue a request. A client cannot repair a broken
/// cursor: it starts again from the first page. The exception is
/// [`CursorError::OtherQuery`], where the request changed and not the
/// cursor.
#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    /// The text is empty. A client that has no cursor does not send one.
    #[error("the cursor is empty")]
    Empty,
    /// The text is not base32.
    #[error("the cursor is not base32 text")]
    Text(#[from] data_encoding::DecodeError),
    /// The bytes are not a cursor message.
    #[error("the cursor is not a cursor of this service")]
    Message(#[from] prost::DecodeError),
    /// The cursor is of a version that this service does not know, for
    /// example from a newer service.
    #[error("cursor format version {found} is not known to this service")]
    Version {
        /// The version in the cursor.
        found: u32,
    },
    /// The cursor is for another request. Send the request of the first
    /// page again, with this cursor.
    #[error("the cursor is for another request")]
    OtherQuery,
    /// The cursor tells no position.
    #[error("the cursor has no position")]
    NoPosition,
    /// The day of a file is not a day.
    #[error("the cursor has an invalid day")]
    Day(#[from] DayError),
    /// The window of the cursor is not one that a lookup can have.
    #[error("the cursor has an invalid window")]
    Window(#[from] WindowError),
    /// The position is at a time out of the window, thus no page gave it.
    #[error("the cursor position is out of its window")]
    OutOfWindow,
}
