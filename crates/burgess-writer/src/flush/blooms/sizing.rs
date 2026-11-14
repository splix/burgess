//! The size of the filters of a new day.

use std::num::NonZeroU64;

use burgess_core::storage::BloomSize;

use super::config::BloomConfig;

/// The size of a new filter, after days whose busiest held `busiest` keys:
/// that many times the multiplier, at the target rate, and at least the
/// floor.
///
/// The count comes from storage, thus a damaged one can ask for a filter
/// larger than a filter can be. Then the filter gets the largest size, at
/// a higher rate, rather than refuse every flush of the day.
pub(super) fn new_day_size(config: &BloomConfig, busiest: u64) -> BloomSize {
    // Saturates in the cast, which the limit then takes in.
    let wanted = (busiest as f64 * config.multiplier).ceil() as u64;
    let size = BloomSize {
        capacity: NonZeroU64::new(wanted.max(1)).expect("at least 1"),
        target_fpp: config.target_fpp,
        min_bytes: config.min_bytes,
    };
    let within = size.within_limit();
    if within != size {
        tracing::warn!(
            busiest,
            capacity = within.capacity.get(),
            "the keys of the days before need a filter over the largest size; using the largest"
        );
    }
    within
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_busiest_day_with_room_to_grow_sizes_the_filter() {
        let config = BloomConfig::default();
        let size = new_day_size(&config, 1_000);
        assert_eq!(size.capacity.get(), 1_500);
        assert_eq!(size.min_bytes, config.min_bytes);
    }

    /// A damaged count must not stop every flush of the day.
    #[test]
    fn a_count_over_the_largest_size_gets_the_largest_size() {
        let config = BloomConfig::default();
        let size = new_day_size(&config, u64::MAX);
        assert_eq!(size, size.within_limit());
        assert!(size.capacity.get() < u64::MAX / 2, "{size:?}");
    }
}
