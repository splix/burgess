//! Storing a batch: data files, then the day manifest, then the checkpoint.
//!
//! The order is what keeps storage consistent without transactions. Readers
//! see only what the manifest lists, so data files written before a crash
//! stay invisible. The checkpoint comes last, so a crash before it only makes
//! the writer read the same records again, never skip any.

mod bounds;
mod checkpoint;
mod clock;
mod error;
mod flusher;

pub use checkpoint::Checkpoint;
pub use clock::{Clock, SystemClock};
pub use error::FlushError;
pub use flusher::Flusher;

#[cfg(test)]
mod tests;
