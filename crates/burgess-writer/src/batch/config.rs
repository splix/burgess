//! When a batch ends.

use std::num::NonZeroUsize;
use std::time::Duration;

/// Bounds that end a batch. Whichever is reached first triggers the flush.
///
/// Together they trade file count against memory and freshness: longer and
/// bigger batches give fewer, larger files, but hold more in memory and make
/// records searchable later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchConfig {
    /// Longest a batch stays open, counted from when it starts. A batch that
    /// received nothing is flushed too, so an idle writer still flushes on
    /// schedule. Must not be zero, which would flush empty batches
    /// continuously; an interval too long to represent never ends a batch.
    pub max_interval: Duration,
    /// Largest a batch grows, in bytes of the Kafka messages it keeps. Skipped
    /// messages are not counted, since they hold no memory. The last message
    /// may take a batch past the bound by its own size.
    pub max_bytes: NonZeroUsize,
}

/// Why a [`BatchConfig`] cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BatchConfigError {
    /// `max_interval` is zero.
    #[error("batch max interval must not be zero")]
    ZeroInterval,
}

impl BatchConfig {
    pub(super) fn check(&self) -> Result<(), BatchConfigError> {
        if self.max_interval.is_zero() {
            return Err(BatchConfigError::ZeroInterval);
        }
        Ok(())
    }
}

impl Default for BatchConfig {
    /// Ten minutes or 256 MiB, provisional until measured on real traffic.
    fn default() -> Self {
        BatchConfig {
            max_interval: Duration::from_secs(10 * 60),
            max_bytes: NonZeroUsize::new(256 * 1024 * 1024).expect("not zero"),
        }
    }
}
