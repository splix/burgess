//! Why reading could not continue from the checkpoint.

use std::error::Error;

use super::position::PartitionId;

/// Why [`PartitionedLog::resume`] failed.
///
/// [`PartitionedLog::resume`]: super::PartitionedLog::resume
#[derive(Debug, thiserror::Error)]
pub enum ResumeError<E: Error + 'static> {
    /// The log itself failed.
    #[error(transparent)]
    Log(E),
    /// The checkpoint has positions for partitions the log does not have.
    #[error("the log has no partitions {}, which the checkpoint has positions for", list(.0))]
    UnknownPartitions(Vec<PartitionId>),
}

fn list(partitions: &[PartitionId]) -> String {
    partitions
        .iter()
        .map(PartitionId::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
