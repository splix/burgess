//! The times that can bound a lookup.
//!
//! A request and a cursor give the bounds of a lookup. Both are checked
//! with this one rule, thus a cursor cannot hold a window that a request
//! cannot have.

use burgess_core::storage::{Day, DayError};
use jiff::Timestamp;

/// Why a time cannot bound a lookup.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LookupTimeError {
    /// The unix milliseconds are out of the range of times.
    #[error("{ms} is out of the range of times")]
    Ms {
        /// The value given.
        ms: i64,
    },
    /// The time is on no day that can have stored files. A lookup reads the
    /// days of its window, thus it would find nothing and give no cause.
    #[error("{time} is out of the range of days")]
    Day {
        /// The time given.
        time: Timestamp,
        /// Why it is on no day.
        #[source]
        source: DayError,
    },
}

/// The time `ms` in unix milliseconds, if it can bound a lookup.
pub fn from_ms(ms: i64) -> Result<Timestamp, LookupTimeError> {
    let time = Timestamp::from_millisecond(ms).map_err(|_| LookupTimeError::Ms { ms })?;
    on_a_day(time)
}

/// `time`, if it can bound a lookup.
pub fn on_a_day(time: Timestamp) -> Result<Timestamp, LookupTimeError> {
    Day::utc(time).map_err(|source| LookupTimeError::Day { time, source })?;
    Ok(time)
}
