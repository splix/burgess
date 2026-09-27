//! Where the writer gets the time that names its objects.

use jiff::Timestamp;

/// The writer's clock. Flush times name the stored objects, so tests supply
/// their own clock to get predictable names.
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
