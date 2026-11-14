//! The window of times that a cursor carries.

use burgess_core::wire::UnixMillis;
use jiff::Timestamp;

use crate::lookup_time::{self, LookupTimeError};

/// The times of the rows that an answer can give, made absolute on its
/// first page.
///
/// The fields are private: a window is made only through [`Window::new`],
/// thus each window that a cursor carries can be read back the same.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    since: Option<Timestamp>,
    until: Option<Timestamp>,
}

/// Why times cannot be the window of a cursor.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WindowError {
    /// The cursor keeps whole milliseconds, like each stored time. A part
    /// of a millisecond would be lost, and the next page would get rows
    /// that the first page did not.
    #[error("{0} is not in whole milliseconds")]
    NotWholeMs(Timestamp),
    /// A time cannot bound a lookup.
    #[error(transparent)]
    Time(#[from] LookupTimeError),
    /// The window ends before it starts.
    #[error("since {since} is after until {until}")]
    Reversed {
        /// The start.
        since: Timestamp,
        /// The end.
        until: Timestamp,
    },
}

impl Window {
    /// The window from `since` to `until`, both included. `None` is no
    /// bound on that side.
    pub fn new(since: Option<Timestamp>, until: Option<Timestamp>) -> Result<Window, WindowError> {
        let since = since.map(checked).transpose()?;
        let until = until.map(checked).transpose()?;
        if let (Some(since), Some(until)) = (since, until)
            && since > until
        {
            return Err(WindowError::Reversed { since, until });
        }
        Ok(Window { since, until })
    }

    /// The earliest time, or `None` when the window has no start.
    pub fn since(&self) -> Option<Timestamp> {
        self.since
    }

    /// The latest time, or `None` when the window has no end.
    pub fn until(&self) -> Option<Timestamp> {
        self.until
    }

    /// Whether a row at `time` can be in the window.
    pub(super) fn holds(&self, time: UnixMillis) -> bool {
        // As in the order of states: a time after the last timestamp is
        // that last timestamp.
        let time = time.saturating_timestamp();
        self.since.is_none_or(|since| since <= time) && self.until.is_none_or(|until| time <= until)
    }

    /// The window of a cursor, from its times in unix milliseconds.
    pub(super) fn from_ms(
        since_ms: Option<i64>,
        until_ms: Option<i64>,
    ) -> Result<Window, WindowError> {
        let since = since_ms.map(lookup_time::from_ms).transpose()?;
        let until = until_ms.map(lookup_time::from_ms).transpose()?;
        Window::new(since, until)
    }
}

/// `time`, if a cursor can keep it.
fn checked(time: Timestamp) -> Result<Timestamp, WindowError> {
    if time.subsec_nanosecond() % 1_000_000 != 0 {
        return Err(WindowError::NotWholeMs(time));
    }
    Ok(lookup_time::on_a_day(time)?)
}
