//! Reading the rows of one tree, or the states that pass a filter, from one
//! states file.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicUsize};

use arrow_array::RecordBatch;
use burgess_core::flow::TreeId;
use burgess_core::storage::{FileStats, ObjectName, StateRow, StatesColumn};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::basic::Type as PhysicalType;

use super::decode::StatesBatch;
use super::error::{StatesFileProblem, StatesReadError};
use super::filter::StatesFilter;
use super::stored_state::StoredState;
use crate::parquet_file::{ColumnKey, FileProblem, FileReadConfig, OpenFile, leaf_column};

/// Reads the rows of a tree, or the states that pass a filter, out of
/// states files, one file at a time.
///
/// It is told the file size by the manifest rather than asking the store,
/// so the footer comes with the first request and no file is ever listed or
/// probed with a HEAD.
#[derive(Clone)]
pub struct StatesFileReader {
    store: Arc<dyn ObjectStore>,
    config: FileReadConfig,
}

impl StatesFileReader {
    /// A reader of the states files in `store`.
    pub fn new(store: Arc<dyn ObjectStore>, config: FileReadConfig) -> Self {
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
    ) -> Result<Vec<StateRow>, StatesReadError> {
        self.read(file, stats, tree)
            .await
            .map_err(|problem| StatesReadError::new(file, problem))
    }

    /// The states of `file`, which the manifest lists with `stats`, that
    /// pass `filter`, in the order the file stores them, up to `limit`.
    ///
    /// Splits are not states, thus they are never kept. The payload pages
    /// are fetched only where a row passes the filters on the other columns.
    /// The read stops at `limit` states that pass all filters, thus a file
    /// of many states costs no more than the page needs.
    pub async fn states(
        &self,
        file: &ObjectName,
        stats: FileStats,
        filter: &Arc<StatesFilter>,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredState>, StatesReadError> {
        self.scan(file, stats, filter, limit)
            .await
            .map_err(|problem| StatesReadError::new(file, problem))
    }

    async fn read(
        &self,
        file: &ObjectName,
        stats: FileStats,
        tree: TreeId,
    ) -> Result<Vec<StateRow>, StatesFileProblem> {
        let open = self.open(file, stats).await?;
        let keys = [ColumnKey {
            column: tree_column(open.metadata())?,
            value: tree.as_bytes(),
        }];
        let Some(mut batches) = open.select(&keys)? else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            rows.extend(StatesBatch::try_from(&batch)?.rows_of(tree)?);
        }
        Ok(rows)
    }

    async fn scan(
        &self,
        file: &ObjectName,
        stats: FileStats,
        filter: &Arc<StatesFilter>,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredState>, StatesFileProblem> {
        let open = self.open(file, stats).await?;
        // The columns are checked before any row is read, thus a file of
        // another layout is a problem whether or not a row passes.
        let schema = Arc::clone(open.metadata().schema());
        StatesBatch::try_from(&RecordBatch::new_empty(schema))?;
        let untested = Arc::new(AtomicUsize::new(0));
        let row_filter = filter.row_filter(open.metadata(), &untested);
        let mut batches = open.filter(row_filter, limit)?;
        let mut states = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            states.extend(StatesBatch::try_from(&batch)?.states()?);
        }
        let untested = untested.load(atomic::Ordering::Relaxed);
        if untested > 0 {
            // The answer has no problem for these, since the states may not
            // pass the filter, but the operator can see that some were not
            // tested.
            tracing::warn!(%file, untested, "payloads that are not JSON were not tested by the filter");
        }
        Ok(states)
    }

    async fn open(&self, file: &ObjectName, stats: FileStats) -> Result<OpenFile, FileProblem> {
        let store = Arc::clone(&self.store);
        OpenFile::open(store, file, stats, self.config.tail_bytes.get()).await
    }
}

/// The leaf index of the tree column, which must have the width of a tree
/// id.
fn tree_column(metadata: &ArrowReaderMetadata) -> Result<usize, FileProblem> {
    leaf_column(
        metadata,
        StatesColumn::Tree.name(),
        PhysicalType::FIXED_LEN_BYTE_ARRAY,
        Some(TreeId::LEN),
    )
}
