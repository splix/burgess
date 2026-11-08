//! Opening a data file at the pages that can hold a key.

use std::sync::Arc;

use burgess_core::storage::{FileStats, ObjectName};
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::file::metadata::PageIndexPolicy;

use super::checks::{check_layout, check_row_count};
use super::problem::FileProblem;
use super::selection::{ColumnKey, KeySelection};
use super::stored_file::StoredFile;

/// The decoded batches of the selected pages of a file.
pub(crate) type Batches = ParquetRecordBatchStream<StoredFile>;

/// The batches of `file` that can hold a key, or `None` when the statistics
/// rule out every row.
///
/// The first request gets `tail_bytes` from the end of the file. The file
/// must have a known layout and the row count that the manifest lists in
/// `stats`. `keys` gives the key columns and values, found in the schema of
/// the file.
pub(crate) async fn open_selected<'k>(
    store: Arc<dyn ObjectStore>,
    file: &ObjectName,
    stats: FileStats,
    tail_bytes: usize,
    keys: impl FnOnce(&ArrowReaderMetadata) -> Result<Vec<ColumnKey<'k>>, FileProblem>,
) -> Result<Option<Batches>, FileProblem> {
    let mut input = StoredFile::new(store, Path::from(file.to_string()), stats.bytes, tail_bytes);
    // The offset index is what lets the decoder fetch single pages rather
    // than whole column chunks. A file without one is still read, a row
    // group at a time.
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let metadata = ArrowReaderMetadata::load_async(&mut input, options).await?;
    check_layout(metadata.metadata())?;
    check_row_count(metadata.metadata(), stats)?;
    let keys = keys(&metadata)?;
    let Some(selection) = KeySelection::find(metadata.metadata(), &keys) else {
        return Ok(None);
    };
    let batches = ParquetRecordBatchStreamBuilder::new_with_metadata(input, metadata)
        .with_row_group_selections(selection.row_groups)
        .build()?;
    Ok(Some(batches))
}
