//! The bloom filters of a day: which days and files can hold a reference,
//! and which states files can hold a tree.
//!
//! A lookup of a reference or a tree without them reads every file of each
//! day. The filters let it skip the days and files that cannot hold the
//! key, at the cost of a small part that it reads for nothing.

mod error;
mod filter;
mod keys;
mod object;
mod size;

#[cfg(test)]
mod tests;

pub use error::BloomError;
pub use keys::{BloomKey, RefsKey, TreeKey};
pub use object::{DayBloom, RefsBloom, TreesBloom};
pub use size::{BloomSize, Fpp, FppError, MAX_BLOOM_BYTES};
