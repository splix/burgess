//! What is wrong with a file, or with reading it.

use burgess_core::storage::SCHEMA_VERSION;
use parquet::errors::ParquetError;

/// What is wrong with a data file, or with reading it, for any kind of
/// data file.
#[derive(Debug, thiserror::Error)]
pub enum FileProblem {
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
    /// The decoder gave a row number that no row can have: none, or below
    /// 0.
    #[error("the decoder gave the row number {found:?}")]
    RowNumber {
        /// The number, if there is one.
        found: Option<i64>,
    },
    /// A column the layout requires is absent or has another type.
    #[error("column {column} is missing or has an unexpected type")]
    Column {
        /// The column name.
        column: &'static str,
    },
}

/// The store's errors reach the reader wrapped in Parquet's, since the
/// decoder fetches the pages; they are unwrapped so callers can tell a store
/// that may recover from a file that will not.
impl From<ParquetError> for FileProblem {
    fn from(error: ParquetError) -> Self {
        match error {
            ParquetError::External(source) => match source.downcast::<object_store::Error>() {
                Ok(store) => FileProblem::Store(*store),
                Err(other) => FileProblem::Parquet(ParquetError::External(other)),
            },
            other => FileProblem::Parquet(other),
        }
    }
}
