//! How large a filter is for the keys it must hold.

use std::f64::consts::LN_2;
use std::num::NonZeroU64;

use super::error::BloomError;

/// The most bits that one key sets. A damaged header with a larger count
/// would make each probe slow, thus a reader refuses it.
pub(super) const MAX_HASHES: u32 = 32;

/// The largest filter, in bytes. The writer and each reader hold a filter
/// in memory, thus a larger one comes from a wrong size, as from a damaged
/// count of keys, and is refused before it is allocated.
pub const MAX_BLOOM_BYTES: u64 = 1 << 30;

/// A false positive rate that a filter is sized for.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Fpp(f64);

/// Why a number is not a false positive rate for a filter.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("{found} is not a false positive rate: use more than 2^-32.5 and less than 1")]
pub struct FppError {
    /// The number given.
    pub found: f64,
}

impl TryFrom<f64> for Fpp {
    type Error = FppError;

    fn try_from(rate: f64) -> Result<Self, Self::Error> {
        // Written so that NaN fails too.
        let valid = rate > 0.0 && rate < 1.0 && hashes_for(rate) <= MAX_HASHES;
        if !valid {
            return Err(FppError { found: rate });
        }
        Ok(Fpp(rate))
    }
}

impl From<Fpp> for f64 {
    fn from(rate: Fpp) -> Self {
        rate.0
    }
}

impl Fpp {
    /// The bits that each key sets. It depends on the rate only: a filter
    /// with more bits than its keys need, as one at its size floor, keeps
    /// the rate with as many hashes, and more hashes only make it slower.
    pub(super) fn hash_count(self) -> u32 {
        hashes_for(self.0)
    }

    /// The most keys that a filter of `bits` bits holds at this rate.
    pub(super) fn keys_held(self, bits: u64) -> u64 {
        (bits as f64 * LN_2 * LN_2 / -self.0.ln()).floor() as u64
    }
}

/// The hashes that give `rate` in a filter of the best size, at least 1.
fn hashes_for(rate: f64) -> u32 {
    // Saturates for a rate so small that the count is out of range.
    (-rate.log2()).round().max(1.0) as u32
}

/// The size of a new filter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BloomSize {
    /// The keys that the filter must hold at its rate.
    pub capacity: NonZeroU64,
    /// The false positive rate at that many keys.
    pub target_fpp: Fpp,
    /// The smallest filter, in bytes. A day whose keys need fewer bytes
    /// gets this many, and so a lower rate.
    pub min_bytes: u64,
}

impl BloomSize {
    /// This size, or the largest size that a filter can have when this one
    /// is larger: the capacity that a filter of [`MAX_BLOOM_BYTES`] holds at
    /// the rate, and a floor of at most that.
    ///
    /// For a capacity that comes from storage, as from the counts of the
    /// days before: a damaged count must not refuse every filter.
    pub fn within_limit(self) -> Self {
        if self.bit_count().is_ok() {
            return self;
        }
        // One key less than the bound, thus rounding in the size cannot
        // take it over the limit.
        let most = self
            .target_fpp
            .keys_held(MAX_BLOOM_BYTES * 8)
            .saturating_sub(1)
            .max(1);
        BloomSize {
            capacity: NonZeroU64::new(most).expect("at least 1"),
            target_fpp: self.target_fpp,
            min_bytes: self.min_bytes.min(MAX_BLOOM_BYTES),
        }
    }

    /// The bits of the filter: what `capacity` keys need at `target_fpp`,
    /// but not less than `min_bytes`, in whole bytes. Refused when that is
    /// more than [`MAX_BLOOM_BYTES`].
    pub(super) fn bit_count(&self) -> Result<u64, BloomError> {
        let keys = self.capacity.get() as f64;
        // Saturates for a size out of the range of `u64`, which is refused.
        let needed = (-keys * self.target_fpp.0.ln() / (LN_2 * LN_2)).ceil() as u64;
        let bytes = needed.div_ceil(8).max(self.min_bytes).max(1);
        if bytes > MAX_BLOOM_BYTES {
            return Err(BloomError::TooLarge {
                bytes,
                max: MAX_BLOOM_BYTES,
            });
        }
        Ok(bytes * 8)
    }
}
