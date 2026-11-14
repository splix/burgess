//! Storing a batch: data files, then the bloom filters of the day, then the
//! day manifest, then the checkpoint.
//!
//! The order is what keeps storage consistent without transactions. Readers
//! see only what the manifest lists, so data files written before a crash
//! stay invisible, and the filters hold the keys of every listed file. The checkpoint comes last, so a crash before it only makes
//! the writer read the same records again, never skip any; on start, the
//! writer drops any listed flush the checkpoint does not cover before it
//! reads those records again.

mod blooms;
mod bounds;
mod checkpoint;
mod clock;
mod error;
mod flusher;
mod restart;
mod seal;

pub use blooms::{BloomConfig, BloomConfigError};
pub use checkpoint::{Checkpoint, PendingFlush};
pub use clock::{Clock, SystemClock};
pub use error::FlushError;
pub use flusher::Flusher;

#[cfg(test)]
mod tests;
