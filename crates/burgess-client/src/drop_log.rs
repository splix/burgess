//! Logging of dropped records without flooding the application's logs.

use std::sync::{Mutex, PoisonError};

use jiff::{SignedDuration, Timestamp};

use crate::dropped::DroppedRecord;

/// Shortest time between two warnings about dropped records.
///
/// A call that is always invalid fails on every request, and a warning each
/// time would bury the application's own logs under ours. One a minute still
/// tells its developers that records are being lost and why.
const WARNING_INTERVAL: SignedDuration = SignedDuration::from_secs(60);

/// Warns about a dropped record at most once per [`WARNING_INTERVAL`], with
/// the number of drops it kept quiet about since the previous warning. The
/// drops in between are logged at debug level.
#[derive(Debug, Default)]
pub(crate) struct DropLog {
    window: Mutex<Window>,
}

#[derive(Debug, Default)]
struct Window {
    last_warning: Option<Timestamp>,
    suppressed: u64,
}

impl DropLog {
    pub fn log(&self, now: Timestamp, dropped: &DroppedRecord) {
        let DroppedRecord {
            service,
            category,
            kind,
            flow,
            reason,
        } = dropped;
        match self.admit(now) {
            Some(suppressed) => tracing::warn!(
                service,
                category,
                %kind,
                %flow,
                suppressed,
                "dropping a record: {reason}"
            ),
            None => tracing::debug!(
                service,
                category,
                %kind,
                %flow,
                "dropping a record: {reason}"
            ),
        }
    }

    /// Whether a drop at `now` gets a warning, and if so how many drops went
    /// without one since the previous warning.
    fn admit(&self, now: Timestamp) -> Option<u64> {
        // Every change to the window is complete once made, so a thread that
        // panicked while holding the lock left it consistent.
        let mut window = self.window.lock().unwrap_or_else(PoisonError::into_inner);
        let due = window.last_warning.is_none_or(|last| {
            let elapsed = now.duration_since(last);
            // A clock stepped back would otherwise silence warnings until it
            // caught up again.
            elapsed.is_negative() || elapsed >= WARNING_INTERVAL
        });
        if due {
            window.last_warning = Some(now);
            Some(std::mem::take(&mut window.suppressed))
        } else {
            window.suppressed += 1;
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_second(1_790_000_000 + seconds).unwrap()
    }

    #[test]
    fn first_drop_warns() {
        assert_eq!(DropLog::default().admit(at(0)), Some(0));
    }

    #[test]
    fn drops_within_a_minute_stay_quiet() {
        let log = DropLog::default();
        log.admit(at(0));
        assert_eq!(log.admit(at(1)), None);
        assert_eq!(log.admit(at(59)), None);
    }

    #[test]
    fn next_warning_counts_the_quiet_drops() {
        let log = DropLog::default();
        log.admit(at(0));
        log.admit(at(1));
        log.admit(at(2));
        assert_eq!(log.admit(at(60)), Some(2));
        assert_eq!(log.admit(at(61)), None);
    }

    #[test]
    fn clock_stepped_back_warns() {
        let log = DropLog::default();
        log.admit(at(0));
        assert_eq!(log.admit(at(-3600)), Some(0));
    }
}
