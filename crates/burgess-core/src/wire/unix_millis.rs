//! The time of a record as it travels on the wire.

use jiff::Timestamp;

/// Unix milliseconds from the producer clock.
///
/// Kept as the raw number rather than a [`Timestamp`] because producers may
/// send values no timestamp can hold, and those records are still stored as
/// sent: judging which times are plausible is left to validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnixMillis(u64);

impl UnixMillis {
    /// The time as a [`Timestamp`], or `None` when it is beyond the latest
    /// time a timestamp can hold.
    pub fn to_timestamp(self) -> Option<Timestamp> {
        i64::try_from(self.0)
            .ok()
            .and_then(|millis| Timestamp::from_millisecond(millis).ok())
    }
}

/// A time before 1970 becomes the epoch, because unix milliseconds on the wire
/// are unsigned and a producer must never fail over its own clock.
impl From<Timestamp> for UnixMillis {
    fn from(time: Timestamp) -> Self {
        UnixMillis(u64::try_from(time.as_millisecond()).unwrap_or(0))
    }
}

impl From<u64> for UnixMillis {
    fn from(millis: u64) -> Self {
        UnixMillis(millis)
    }
}

impl From<UnixMillis> for u64 {
    fn from(time: UnixMillis) -> Self {
        time.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trip_at_millisecond_precision() {
        let time: Timestamp = "2026-09-27T10:00:00.123Z".parse().unwrap();
        assert_eq!(UnixMillis::from(time).to_timestamp(), Some(time));
    }

    #[test]
    fn time_before_epoch_is_epoch() {
        let time = Timestamp::from_millisecond(-5).unwrap();
        assert_eq!(u64::from(UnixMillis::from(time)), 0);
    }

    #[test]
    fn implausible_time_is_kept_but_has_no_timestamp() {
        let time = UnixMillis::from(u64::MAX);
        assert_eq!(u64::from(time), u64::MAX);
        assert_eq!(time.to_timestamp(), None);
    }
}
