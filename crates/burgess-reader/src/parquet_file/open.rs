//! Opening a data file, then reading the pages that can hold a key or the
//! rows that pass a filter.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::storage::{FileStats, ObjectName};
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::file::metadata::PageIndexPolicy;

use super::checks::{check_layout, check_row_count};
use super::problem::FileProblem;
use super::row_number::row_number_field;
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
        let options = ArrowReaderOptions::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .with_virtual_columns(vec![row_number_field()])?;
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

    /// The batches of the rows that pass `filter`, from all row groups, up
    /// to `limit` rows if one is given. With `after`, only the rows after
    /// the row of that number are read.
    ///
    /// The decoder reads the columns of the filter first, and the other
    /// columns only for the pages that hold a row that passes. Rows up to
    /// `after` are skipped before the filter, thus the limit counts only
    /// the rows after it.
    pub(crate) fn filter(
        self,
        filter: RowFilter,
        limit: Option<NonZeroUsize>,
        after: Option<u64>,
    ) -> Result<Batches, FileProblem> {
        let rows = self.metadata.metadata().file_metadata().num_rows();
        let mut builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(self.input, self.metadata)
                .with_row_filter(filter);
        if let Some(after) = after {
            builder = builder.with_row_selection(rows_after(after, rows));
        }
        if let Some(limit) = limit {
            builder = builder.with_limit(limit.get());
        }
        Ok(builder.build()?)
    }
}

/// The rows after the row `after` of a file of `rows` rows.
fn rows_after(after: u64, rows: i64) -> RowSelection {
    let rows = usize::try_from(rows).unwrap_or(0);
    let skipped = usize::try_from(after)
        .map_or(rows, |after| after.saturating_add(1))
        .min(rows);
    RowSelection::from(vec![
        RowSelector::skip(skipped),
        RowSelector::select(rows - skipped),
    ])
}
