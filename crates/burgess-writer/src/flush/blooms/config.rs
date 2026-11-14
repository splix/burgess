//! How the writer sizes the bloom filters of a day.

use std::num::NonZeroU32;

use burgess_core::storage::{Fpp, MAX_BLOOM_BYTES};

/// The longest window. Each day of it is read when a day starts, and
/// retention keeps about a month of days.
pub const MAX_WINDOW_DAYS: u32 = 31;

/// How the writer sizes the bloom filters of a day.
///
/// The keys of a day are known only at its end, but a filter has its size
/// from its first flush. Thus the size comes from the days before: the most
/// keys of one of them, with room to grow.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BloomConfig {
    /// The false positive rate that a filter is sized for.
    pub target_fpp: Fpp,
    /// The days before a new day whose keys size its filters.
    pub window_days: NonZeroU32,
    /// The room to grow: the capacity is the most keys of a day of the
    /// window, times this.
    pub multiplier: f64,
    /// The smallest filter, in bytes. A quiet day still gets a filter large
    /// enough for a busy one, at a lower rate.
    pub min_bytes: u64,
}

/// Why the bloom settings cannot be used.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum BloomConfigError {
    /// The multiplier gives no capacity, or one that is not a number.
    #[error("bloom multiplier {found} is not a number above 0")]
    Multiplier {
        /// The multiplier given.
        found: f64,
    },
    /// The window reads more days than retention keeps.
    #[error("bloom window_days {found} is over the maximum of {MAX_WINDOW_DAYS}")]
    WindowDays {
        /// The window given.
        found: u32,
    },
    /// Every filter would be larger than a filter can be.
    #[error("bloom min_bytes {found} is over the maximum of {MAX_BLOOM_BYTES}")]
    MinBytes {
        /// The size given.
        found: u64,
    },
}

impl BloomConfig {
    /// Whether the settings can size a filter.
    pub fn check(&self) -> Result<(), BloomConfigError> {
        // Written so that NaN fails too.
        if !(self.multiplier > 0.0 && self.multiplier.is_finite()) {
            return Err(BloomConfigError::Multiplier {
                found: self.multiplier,
            });
        }
        if self.window_days.get() > MAX_WINDOW_DAYS {
            return Err(BloomConfigError::WindowDays {
                found: self.window_days.get(),
            });
        }
        if self.min_bytes > MAX_BLOOM_BYTES {
            return Err(BloomConfigError::MinBytes {
                found: self.min_bytes,
            });
        }
        Ok(())
    }
}

impl Default for BloomConfig {
    /// A false positive rate of 1%, the busiest of the 3 days before with
    /// half as many keys again, and at least 1 MiB.
    fn default() -> Self {
        BloomConfig {
            target_fpp: Fpp::try_from(0.01).expect("1% is a rate"),
            window_days: NonZeroU32::new(3).expect("not zero"),
            multiplier: 1.5,
            min_bytes: 1 << 20,
        }
    }
}
