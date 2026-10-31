//! Where the emitter gets the time of its records.

use jiff::Timestamp;

/// The producer's clock. It stamps every record and the start of every tree,
/// so tests supply their own clock to get predictable records.
pub trait Clock: Send + Sync {
    /// The current time.
    fn now(&self) -> Timestamp;
}

/// The host's clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }
}
