//! Reading the rows of one tree from one states file.

use std::sync::Arc;

use burgess_core::flow::TreeId;
use burgess_core::storage::{FileStats, ObjectName, StateRow, StatesColumn};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::basic::Type as PhysicalType;

use super::decode::StatesBatch;
use super::error::{StatesFileProblem, TreeReadError};
use crate::parquet_file::{ColumnKey, FileReadConfig, leaf_column, open_selected};

/// Reads trees out of states files, one file at a time.
///
/// It is told the file size by the manifest rather than asking the store,
/// so the footer comes with the first request and no file is ever listed or
/// probed with a HEAD.
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
        let keys = |metadata: &ArrowReaderMetadata| {
            let column = leaf_column(
                metadata,
                StatesColumn::Tree.name(),
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
                Some(TreeId::LEN),
            )?;
            Ok(vec![ColumnKey {
                column,
                value: tree.as_bytes(),
            }])
        };
        let store = Arc::clone(&self.store);
        let tail_bytes = self.config.tail_bytes.get();
        let Some(mut batches) = open_selected(store, file, stats, tail_bytes, keys).await? else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            rows.extend(StatesBatch::try_from(&batch)?.rows_of(tree)?);
        }
        Ok(rows)
    }
}
