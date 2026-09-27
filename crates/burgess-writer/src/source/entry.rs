//! One message read from a log.

use super::position::{PartitionId, Position};

/// One message as the log delivered it, before it is decoded as a record.
///
/// Decoding happens later so that a malformed message can be skipped and
/// logged without the log itself failing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The partition the entry was read from.
    pub partition: PartitionId,
    /// The message body.
    pub payload: Vec<u8>,
    /// Where to resume to read the entries after this one. The writer
    /// checkpoints this once the entry is safely stored.
    pub resume_at: Position,
}
