//! Warning about repeated failures without flooding the application's logs.

use std::sync::{Mutex, PoisonError};

use jiff::{SignedDuration, Timestamp};

/// Shortest time between two warnings from one [`Throttle`].
///
/// A call that is always invalid, or a broker that is down, fails on every
/// request, and a warning each time would bury the application's own logs
/// under ours. One a minute still tells its developers that records are being
/// lost and why.
const WARNING_INTERVAL: SignedDuration = SignedDuration::from_secs(60);

/// How loudly to log one failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Log a warning, mentioning the failures kept quiet since the last one.
    Warn {
        /// Failures logged quietly since the previous warning.
        suppressed: u64,
    },
    /// Log at debug level only: a warning went out less than a minute ago.
    Quiet,
}

/// Lets one warning through per [`WARNING_INTERVAL`] and counts the
/// failures in between.
#[derive(Debug, Default)]
pub(crate) struct Throttle {
    window: Mutex<Window>,
}

#[derive(Debug, Default)]
struct Window {
    last_warning: Option<Timestamp>,
    suppressed: u64,
}

impl Throttle {
    /// How loudly to log a failure that happened at `now`.
    pub fn admit(&self, now: Timestamp) -> Verdict {
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
            Verdict::Warn {
                suppressed: std::mem::take(&mut window.suppressed),
            }
        } else {
            window.suppressed += 1;
            Verdict::Quiet
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_second(1_790_000_000 + seconds).unwrap()
    }

    fn warn(suppressed: u64) -> Verdict {
        Verdict::Warn { suppressed }
    }

    #[test]
    fn first_failure_warns() {
        assert_eq!(Throttle::default().admit(at(0)), warn(0));
    }

    #[test]
    fn failures_within_a_minute_stay_quiet() {
        let throttle = Throttle::default();
        throttle.admit(at(0));
        assert_eq!(throttle.admit(at(1)), Verdict::Quiet);
        assert_eq!(throttle.admit(at(59)), Verdict::Quiet);
    }

    #[test]
    fn next_warning_counts_the_quiet_failures() {
        let throttle = Throttle::default();
        throttle.admit(at(0));
        throttle.admit(at(1));
        throttle.admit(at(2));
        assert_eq!(throttle.admit(at(60)), warn(2));
        assert_eq!(throttle.admit(at(61)), Verdict::Quiet);
    }

    #[test]
    fn clock_stepped_back_warns() {
        let throttle = Throttle::default();
        throttle.admit(at(0));
        assert_eq!(throttle.admit(at(-3600)), warn(0));
    }
}
