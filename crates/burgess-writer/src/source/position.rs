//! Where in a log reading starts or resumes.

use std::fmt;

/// Identifies one partition of a log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId(u32);

impl From<u32> for PartitionId {
    fn from(value: u32) -> Self {
        PartitionId(value)
    }
}

impl From<PartitionId> for u32 {
    fn from(value: PartitionId) -> Self {
        value.0
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A point in one partition, meaning "resume reading here".
///
/// Opaque to the writer: only the log that handed out a position knows what
/// it means, so the writer stores and returns positions but never computes
/// them. The number inside exists so a position can be written into the
/// checkpoint and read back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Position(u64);

impl From<u64> for Position {
    fn from(value: u64) -> Self {
        Position(value)
    }
}

impl From<Position> for u64 {
    fn from(value: Position) -> Self {
        value.0
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Where to start reading a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// The oldest entry the log still holds, for a partition the writer has
    /// never checkpointed.
    Earliest,
    /// A position handed out by the log earlier, usually from the checkpoint.
    At(Position),
}
