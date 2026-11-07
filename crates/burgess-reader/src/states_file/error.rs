//! Why a tree could not be read from a states file.

use burgess_core::flow::Subflow;
use burgess_core::storage::{ObjectName, SCHEMA_VERSION};
use parquet::errors::ParquetError;

use super::decode::InvalidRow;

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
    /// The object store failed. Unlike the other problems this one may pass,
    /// and a file the manifest lists may also be gone because retention
    /// deleted it meanwhile.
    #[error("the object store failed")]
    Store(#[source] object_store::Error),
    /// The file is not valid Parquet.
    #[error("the file is not valid parquet")]
    Parquet(#[source] ParquetError),
    /// The file was written in a layout this reader does not know, so its
    /// columns may mean something else.
    #[error("the file has layout version {found:?}, this reader knows {SCHEMA_VERSION}")]
    UnknownLayout {
        /// The version the file declares, if any.
        found: Option<String>,
    },
    /// The file is not the one the manifest lists under its name.
    #[error("the file has {found} rows, the manifest lists {listed}")]
    RowCount {
        /// Rows the manifest lists.
        listed: u64,
        /// Rows the file holds.
        found: i64,
    },
    /// A column the layout requires is absent or has another type.
    #[error("column {column} is missing or has an unexpected type")]
    Column {
        /// The column name.
        column: &'static str,
    },
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

/// The store's errors reach the reader wrapped in Parquet's, since the
/// decoder fetches the pages; they are unwrapped so callers can tell a store
/// that may recover from a file that will not.
impl From<ParquetError> for StatesFileProblem {
    fn from(error: ParquetError) -> Self {
        match error {
            ParquetError::External(source) => match source.downcast::<object_store::Error>() {
                Ok(store) => StatesFileProblem::Store(*store),
                Err(other) => StatesFileProblem::Parquet(ParquetError::External(other)),
            },
            other => StatesFileProblem::Parquet(other),
        }
    }
}
