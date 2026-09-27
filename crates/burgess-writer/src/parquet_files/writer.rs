//! Encoding a batch into its two Parquet files.

use arrow_schema::ArrowError;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;

use super::config::ParquetConfig;
use super::layout::FileLayout;
use crate::batch::{RefRow, SortedBatch, StateRow};

/// Why a file could not be written.
#[derive(Debug, thiserror::Error)]
pub enum FileError {
    /// The rows could not be arranged as Arrow columns.
    #[error("building columns failed")]
    Arrow(#[from] ArrowError),
    /// The Parquet encoder failed, or the settings are invalid.
    #[error("encoding parquet failed")]
    Parquet(#[from] ParquetError),
}

/// One encoded Parquet file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFile {
    /// The file contents.
    pub bytes: Vec<u8>,
    /// Number of rows in it.
    pub rows: usize,
}

/// The two files a batch is stored as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchFiles {
    /// States and splits.
    pub states: ParquetFile,
    /// References.
    pub refs: ParquetFile,
}

/// Encodes batches as Parquet files with the configured settings.
///
/// Takes only whole [`SortedBatch`]es: each file records that its rows are
/// sorted, and readers skip pages based on it, so rows must never reach a
/// file in any other order.
pub struct FileWriter {
    pub(super) states: FileLayout<StateRow>,
    pub(super) refs: FileLayout<RefRow>,
}

impl FileWriter {
    /// Prepares both file layouts, so invalid settings fail at startup rather
    /// than at the first flush.
    pub fn new(config: ParquetConfig) -> Result<Self, FileError> {
        let compression = Compression::ZSTD(ZstdLevel::try_new(config.zstd_level)?);
        Ok(FileWriter {
            states: FileLayout::new(&config, compression)?,
            refs: FileLayout::new(&config, compression)?,
        })
    }

    /// Writes both files of a batch. Each is written even when it has no
    /// rows: readers treat a batch with only one of its files as incomplete.
    pub fn write_batch(&self, batch: &SortedBatch) -> Result<BatchFiles, FileError> {
        Ok(BatchFiles {
            states: self.states.encode(&batch.states)?,
            refs: self.refs.encode(&batch.refs)?,
        })
    }
}
