//! Reading the rows of one tree from one states file.

use std::sync::Arc;

use burgess_core::flow::TreeId;
use burgess_core::storage::{
    FileStats, ObjectName, SCHEMA_VERSION, SCHEMA_VERSION_KEY, StateRow, StatesColumn,
};
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};

use super::config::StatesFileConfig;
use super::decode::StatesBatch;
use super::error::{StatesFileProblem, TreeReadError};
use super::selection::TreeSelection;
use super::stored_file::StoredFile;

/// Reads trees out of states files, one file at a time.
///
/// It is told the file size by the manifest rather than asking the store,
/// so the footer comes with the first request and no file is ever listed or
/// probed with a HEAD.
pub struct StatesFileReader {
    store: Arc<dyn ObjectStore>,
    config: StatesFileConfig,
}

impl StatesFileReader {
    /// A reader of the states files in `store`.
    pub fn new(store: Arc<dyn ObjectStore>, config: StatesFileConfig) -> Self {
        StatesFileReader { store, config }
    }

    /// Every row of `tree` in the states file `file`, which the manifest
    /// lists with `stats`.
    ///
    /// The rows come in `(subflow, ts)` order, the order the file stores
    /// them in, and include splits as well as states, so a caller can follow
    /// a flow into its children. A tree the file does not hold gives no
    /// rows. When the bounds of `tree` rule it out, reading it fetches only
    /// the file's tail; when it sorts between two trees of one page, that
    /// page is fetched too, since bounds cannot tell it apart.
    pub async fn tree(
        &self,
        file: &ObjectName,
        stats: FileStats,
        tree: TreeId,
    ) -> Result<Vec<StateRow>, TreeReadError> {
        self.read(file, stats, tree)
            .await
            .map_err(|problem| TreeReadError::new(file, problem))
    }

    async fn read(
        &self,
        file: &ObjectName,
        stats: FileStats,
        tree: TreeId,
    ) -> Result<Vec<StateRow>, StatesFileProblem> {
        let mut input = StoredFile::new(
            Arc::clone(&self.store),
            Path::from(file.to_string()),
            stats.bytes,
            self.config.tail_bytes.get(),
        );
        // The offset index is what lets the decoder fetch single pages
        // rather than whole column chunks. A file without one is still read,
        // a row group at a time.
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let metadata = ArrowReaderMetadata::load_async(&mut input, options).await?;
        check_layout(metadata.metadata())?;
        check_row_count(metadata.metadata(), stats)?;
        let Some(selection) =
            TreeSelection::find(metadata.metadata(), tree_column(&metadata)?, tree)
        else {
            return Ok(Vec::new());
        };
        let mut batches = ParquetRecordBatchStreamBuilder::new_with_metadata(input, metadata)
            .with_row_group_selections(selection.row_groups)
            .build()?;
        let mut rows = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            rows.extend(StatesBatch::try_from(&batch)?.rows_of(tree)?);
        }
        Ok(rows)
    }
}

fn check_layout(metadata: &ParquetMetaData) -> Result<(), StatesFileProblem> {
    let found = metadata
        .file_metadata()
        .key_value_metadata()
        .and_then(|entries| entries.iter().find(|entry| entry.key == SCHEMA_VERSION_KEY))
        .and_then(|entry| entry.value.clone());
    match found {
        Some(version) if version == SCHEMA_VERSION => Ok(()),
        found => Err(StatesFileProblem::UnknownLayout { found }),
    }
}

fn check_row_count(metadata: &ParquetMetaData, stats: FileStats) -> Result<(), StatesFileProblem> {
    let found = metadata.file_metadata().num_rows();
    if u64::try_from(found).is_ok_and(|found| found == stats.rows) {
        Ok(())
    } else {
        Err(StatesFileProblem::RowCount {
            listed: stats.rows,
            found,
        })
    }
}

/// The leaf index of `tree`, which the statistics are looked up by.
///
/// Its width is checked here because a wider id column would compare
/// against no tree, and every lookup would wrongly find the file empty.
fn tree_column(metadata: &ArrowReaderMetadata) -> Result<usize, StatesFileProblem> {
    let name = StatesColumn::Tree.name();
    metadata
        .parquet_schema()
        .columns()
        .iter()
        .position(|column| {
            column.path().string() == name
                && column.physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY
                && usize::try_from(column.type_length()) == Ok(TreeId::LEN)
        })
        .ok_or(StatesFileProblem::Column { column: name })
}
