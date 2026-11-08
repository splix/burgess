//! Opening a data file, then reading the pages that can hold a key or the
//! rows that pass a filter.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{FileStats, ObjectName};
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowFilter};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::file::metadata::PageIndexPolicy;

use super::checks::{check_layout, check_row_count};
use super::problem::FileProblem;
use super::selection::{ColumnKey, KeySelection};
use super::stored_file::StoredFile;

/// The decoded batches of the selected rows of a file.
pub(crate) type Batches = ParquetRecordBatchStream<StoredFile>;

/// A data file whose footer is read and checked, and whose data pages are
/// not read yet.
pub(crate) struct OpenFile {
    input: StoredFile,
    metadata: ArrowReaderMetadata,
}

impl OpenFile {
    /// Reads the footer of `file`, which must have a known layout and the
    /// row count that the manifest lists in `stats`.
    ///
    /// The first request gets `tail_bytes` from the end of the file.
    pub(crate) async fn open(
        store: Arc<dyn ObjectStore>,
        file: &ObjectName,
        stats: FileStats,
        tail_bytes: usize,
    ) -> Result<Self, FileProblem> {
        let mut input =
            StoredFile::new(store, Path::from(file.to_string()), stats.bytes, tail_bytes);
        // The offset index is what lets the decoder fetch single pages rather
        // than whole column chunks. A file without one is still read, a row
        // group at a time.
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let metadata = ArrowReaderMetadata::load_async(&mut input, options).await?;
        check_layout(metadata.metadata())?;
        check_row_count(metadata.metadata(), stats)?;
        Ok(OpenFile { input, metadata })
    }

    /// The footer, which tells where the columns are.
    pub(crate) fn metadata(&self) -> &ArrowReaderMetadata {
        &self.metadata
    }

    /// The batches of the pages that can hold a row with every value of
    /// `keys`, or `None` when the statistics rule out every row.
    pub(crate) fn select(self, keys: &[ColumnKey<'_>]) -> Result<Option<Batches>, FileProblem> {
        let Some(selection) = KeySelection::find(self.metadata.metadata(), keys) else {
            return Ok(None);
        };
        let batches = ParquetRecordBatchStreamBuilder::new_with_metadata(self.input, self.metadata)
            .with_row_group_selections(selection.row_groups)
            .build()?;
        Ok(Some(batches))
    }

    /// The batches of the first `limit` rows that pass `filter`, from all
    /// row groups.
    ///
    /// The decoder reads the columns of the filter first, and the other
    /// columns only for the pages that hold a row that passes.
    pub(crate) fn filter(
        self,
        filter: RowFilter,
        limit: NonZeroUsize,
    ) -> Result<Batches, FileProblem> {
        let batches = ParquetRecordBatchStreamBuilder::new_with_metadata(self.input, self.metadata)
            .with_row_filter(filter)
            .with_limit(limit.get())
            .build()?;
        Ok(batches)
    }
}
