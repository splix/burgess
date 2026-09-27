//! Where the writer reads records from.
//!
//! The writer depends on a partitioned log with opaque positions rather than
//! on Kafka directly. Kafka is the only production source today, but tests run
//! against an in-memory log (`MemoryLog`, behind the `test-util` feature for
//! other crates), and a later internal buffer is meant to slot in behind the
//! same interface.

mod entry;
#[cfg(any(test, feature = "test-util"))]
mod memory;
mod position;
mod resume_error;

pub use entry::LogEntry;
#[cfg(any(test, feature = "test-util"))]
pub use memory::{MemoryLog, MemoryLogError, MemoryLogReader};
pub use position::{PartitionId, Position, Start};
pub use resume_error::ResumeError;

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroUsize;

/// A log split into partitions, read from positions the writer stores itself.
///
/// The writer keeps no state in the log. It checkpoints the [`Position`]s of
/// what it has flushed, and after a restart it assigns those positions again
/// and continues from there. That is what lets it replay unflushed records
/// after a crash without skipping or duplicating anything already stored.
pub trait PartitionedLog {
    /// Why the log could not be read.
    type Error: Error + Send + Sync + 'static;

    /// All partitions of the log.
    fn partitions(&self) -> impl Future<Output = Result<Vec<PartitionId>, Self::Error>> + Send;

    /// Starts reading each listed partition from its [`Start`]. Replaces any
    /// earlier assignment; partitions not listed are not read.
    ///
    /// Fails for a partition the log does not have, and for a position the log
    /// cannot resume from, rather than silently starting somewhere else.
    fn assign(
        &mut self,
        starts: BTreeMap<PartitionId, Start>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Waits until at least one entry is available, then returns up to `max`
    /// of them, in order within each partition. With nothing assigned it waits
    /// until the future is dropped.
    ///
    /// Cancellation safe: dropping the future before it completes loses no
    /// entries, so the writer can race it against a flush timer.
    fn pull(
        &mut self,
        max: NonZeroUsize,
    ) -> impl Future<Output = Result<Vec<LogEntry>, Self::Error>> + Send;

    /// Assigns every partition, continuing from its `known` position.
    ///
    /// A partition with no known position starts from its earliest entry, so
    /// nothing is missed on the first run, nor in a partition added to the log
    /// since the last checkpoint. A known position for a partition the log
    /// does not have is an error: partitions are never removed, so it means the
    /// checkpoint belongs to another log, and starting over would re-read it
    /// all.
    fn resume(
        &mut self,
        known: &BTreeMap<PartitionId, Position>,
    ) -> impl Future<Output = Result<(), ResumeError<Self::Error>>> + Send
    where
        Self: Send,
    {
        async move {
            let partitions = self.partitions().await.map_err(ResumeError::Log)?;
            let unknown: Vec<PartitionId> = known
                .keys()
                .filter(|partition| !partitions.contains(partition))
                .copied()
                .collect();
            if !unknown.is_empty() {
                return Err(ResumeError::UnknownPartitions(unknown));
            }
            let starts = partitions
                .into_iter()
                .map(|partition| {
                    let start = known
                        .get(&partition)
                        .map_or(Start::Earliest, |at| Start::At(*at));
                    (partition, start)
                })
                .collect();
            self.assign(starts).await.map_err(ResumeError::Log)
        }
    }
}
