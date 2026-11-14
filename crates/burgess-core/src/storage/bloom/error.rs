//! Why bytes are not a bloom filter object that this build can use.

use crate::storage::day::DayError;
use crate::storage::frame::FrameError;
use crate::storage::writer_id::WriterIdError;

use super::size::{FppError, MAX_HASHES};

/// Why bytes are not a bloom filter object that this build can use.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BloomError {
    /// The object is not a well-formed `BGSB` frame.
    #[error(transparent)]
    Frame(#[from] FrameError),
    /// Written by a newer writer, which can hash keys in another way.
    #[error("bloom format version {found} is not supported, expected {expected}")]
    UnsupportedVersion {
        /// The version in the header.
        found: u32,
        /// The version this build reads.
        expected: u32,
    },
    /// The day is not in `YYYYMMDD` form.
    #[error(transparent)]
    Day(#[from] DayError),
    /// The writer id is not a valid one.
    #[error(transparent)]
    WriterId(#[from] WriterIdError),
    /// The filter has no bits, thus no key can be probed.
    #[error("the filter has no bits")]
    NoBits,
    /// The header and the filter bytes tell another size.
    #[error("the header has {header} bits, the filter {filter}")]
    BitCount {
        /// The bits the header tells.
        header: u64,
        /// The bits of the filter bytes.
        filter: u64,
    },
    /// No key sets a bit, or so many that a probe would be slow.
    #[error("the filter sets {found} bits for each key, expected 1 to {MAX_HASHES}")]
    HashCount {
        /// The count in the header.
        found: u32,
    },
    /// The filter would be larger than a filter can be.
    #[error("a filter of {bytes} bytes is larger than the maximum of {max}")]
    TooLarge {
        /// The bytes asked for.
        bytes: u64,
        /// The largest filter.
        max: u64,
    },
    /// The filter is sized for no key.
    #[error("the filter has a capacity of 0")]
    NoCapacity,
    /// The rate the filter is sized for is not a rate.
    #[error(transparent)]
    TargetFpp(#[from] FppError),
}
