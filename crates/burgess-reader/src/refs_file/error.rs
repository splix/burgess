//! Why the references could not be read from a refs file.

use burgess_core::storage::ObjectName;
use parquet::errors::ParquetError;

use crate::parquet_file::FileProblem;

/// Why the references could not be read from a refs file.
#[derive(Debug, thiserror::Error)]
#[error("reading references from {file} failed")]
pub struct RefsReadError {
    file: ObjectName,
    #[source]
    problem: RefsFileProblem,
}

impl RefsReadError {
    pub(super) fn new(file: &ObjectName, problem: impl Into<RefsFileProblem>) -> Self {
        RefsReadError {
            file: file.clone(),
            problem: problem.into(),
        }
    }

    /// The file being read.
    pub fn file(&self) -> &ObjectName {
        &self.file
    }

    /// What went wrong.
    pub fn problem(&self) -> &RefsFileProblem {
        &self.problem
    }
}

/// What is wrong with a refs file, or with reading it.
#[derive(Debug, thiserror::Error)]
pub enum RefsFileProblem {
    /// A problem that any data file can have.
    #[error(transparent)]
    File(#[from] FileProblem),
    /// The writer stores only times from the Unix epoch on.
    #[error("the reference at {ts_millis} ms is before the Unix epoch")]
    TimeBeforeEpoch {
        /// The time as stored.
        ts_millis: i64,
    },
}

impl From<ParquetError> for RefsFileProblem {
    fn from(error: ParquetError) -> Self {
        RefsFileProblem::File(error.into())
    }
}
