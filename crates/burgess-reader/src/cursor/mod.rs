//! The cursor that continues a paged answer.
//!
//! The service keeps no state between pages. Thus the cursor carries all
//! that the next page needs: the last row given and the window of the
//! first page. A window without an end ends at "now", which moves; with the
//! window of the first page, the pages do not drift. It also carries the
//! oldest day that the rest of the answer reads, thus the next page knows
//! when retention deleted a part of the answer.
//!
//! The cursor also carries a hash of its request. Its position has no
//! meaning in another scan, thus a cursor is refused with another request.

mod cursors;
mod error;
mod given;
mod position;
mod query_hash;
mod window;

#[cfg(test)]
mod tests;

pub use cursors::Cursors;
pub use error::CursorError;
pub(crate) use given::{Given, Walk};
pub use position::{Position, Positioned, RowPosition};
pub use query_hash::{PagedMethod, PagedRequest, QueryHash};
pub use window::{Window, WindowError};

use burgess_core::storage::Day;
use data_encoding::{BASE32_NOPAD, Encoding};
use prost::Message;

/// Types generated from `proto/burgess/cursor/v1/cursor.proto`.
///
/// Private, thus other code reads a cursor only through [`Cursor::decode`],
/// which checks what protobuf cannot.
mod proto {
    include!(concat!(env!("OUT_DIR"), "/burgess.cursor.v1.rs"));
}

/// The version of the cursor message. Change it when the meaning of a
/// field changes: a cursor of another version is refused, not misread.
const FORMAT_VERSION: u32 = 1;

/// Base32 without padding has only letters and digits, thus the text goes
/// in a URL or a shell argument as it is.
const TEXT: &Encoding = &BASE32_NOPAD;

/// Where a paged answer stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    /// The window of the first page.
    pub window: Window,
    /// The last row given.
    pub position: Position,
    /// The oldest day that the rest of the answer reads. When retention
    /// deletes it, rows of the answer are gone, thus the cursor expires
    /// even though the file of its row is still stored.
    pub oldest: Day,
}

impl Cursor {
    /// The text of the cursor, for the request with the hash `query`.
    pub fn encode(&self, query: QueryHash) -> String {
        let message = proto::Cursor {
            format_version: FORMAT_VERSION,
            query_hash: query.into(),
            since_ms: self.window.since().map(|since| since.as_millisecond()),
            until_ms: self.window.until().map(|until| until.as_millisecond()),
            position: Some(self.position.into()),
            oldest_day: self.oldest.to_string(),
        };
        TEXT.encode(&message.encode_to_vec())
    }

    /// The cursor in `text`, refused when it is not for the request with
    /// the hash `query`.
    ///
    /// The text comes from the client, thus each field is checked.
    pub fn decode(text: &str, query: QueryHash) -> Result<Cursor, CursorError> {
        // Protobuf reads no bytes as a message of defaults, thus without
        // this an empty text would be refused for its version.
        if text.is_empty() {
            return Err(CursorError::Empty);
        }
        let bytes = TEXT.decode(text.as_bytes())?;
        let message = proto::Cursor::decode(bytes.as_slice())?;
        if message.format_version != FORMAT_VERSION {
            return Err(CursorError::Version {
                found: message.format_version,
            });
        }
        if u64::from(query) != message.query_hash {
            return Err(CursorError::OtherQuery);
        }
        let window = Window::from_ms(message.since_ms, message.until_ms)?;
        let position: Position = message
            .position
            .ok_or(CursorError::NoPosition)?
            .try_into()?;
        if let Position::Ordered { ts, .. } = position
            && !window.holds(ts)
        {
            return Err(CursorError::OutOfWindow);
        }
        let oldest = message.oldest_day.parse()?;
        Ok(Cursor {
            window,
            position,
            oldest,
        })
    }
}
