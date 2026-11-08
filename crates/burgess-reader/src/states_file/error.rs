//! Why a tree could not be read from a states file.

use burgess_core::flow::Subflow;
use burgess_core::storage::ObjectName;
use parquet::errors::ParquetError;

use super::decode::InvalidRow;
use crate::parquet_file::FileProblem;

/// Why a tree could not be read from a states file.
#[derive(Debug, thiserror::Error)]
#[error("reading a tree from {file} failed")]
pub struct TreeReadError {
    file: ObjectName,
    #[source]
    problem: StatesFileProblem,
}

impl TreeReadError {
    pub(super) fn new(file: &ObjectName, problem: impl Into<StatesFileProblem>) -> Self {
        TreeReadError {
            file: file.clone(),
            problem: problem.into(),
        }
    }

    /// The file being read.
    pub fn file(&self) -> &ObjectName {
        &self.file
    }

    /// What went wrong.
    pub fn problem(&self) -> &StatesFileProblem {
        &self.problem
    }
}

/// What is wrong with a states file, or with reading it.
#[derive(Debug, thiserror::Error)]
pub enum StatesFileProblem {
    /// A problem that any data file can have.
    #[error(transparent)]
    File(#[from] FileProblem),
    /// A row breaks the rules the writer follows.
    #[error("the row of subflow {} at {ts_millis} ms is invalid", u32::from(*subflow))]
    Row {
        /// Subflow of the row.
        subflow: Subflow,
        /// Time of the row as stored, which may itself be what is invalid.
        ts_millis: i64,
        /// What is wrong with it.
        #[source]
        problem: InvalidRow,
    },
}

impl From<ParquetError> for StatesFileProblem {
    fn from(error: ParquetError) -> Self {
        StatesFileProblem::File(error.into())
    }
}
