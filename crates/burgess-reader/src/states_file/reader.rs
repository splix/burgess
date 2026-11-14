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
use super::scan_order::{FileKey, ScanOrder};
use super::stored_state::StoredState;
use crate::ordering::{Smallest, TsOrder};
use crate::parquet_file::{
    ColumnKey, FileProblem, FileReadConfig, Numbered, OpenFile, leaf_column,
};

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
    /// lists with `stats`, after the row `after` if given.
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
        after: Option<u64>,
    ) -> Result<Vec<Numbered<StateRow>>, StatesReadError> {
        let mut rows = self
            .read(file, stats, tree)
            .await
            .map_err(|problem| StatesReadError::new(file, problem))?;
        // The pages that can hold the tree are read whole, thus the rows up
        // to `after` are dropped here.
        if let Some(after) = after {
            rows.retain(|row| row.number > after);
        }
        Ok(rows)
    }

    /// The states of `file`, which the manifest lists with `stats`, that
    /// pass `filter`, up to `limit`: the first ones as `order` tells, after
    /// the position it gives.
    ///
    /// Splits are not states, thus they are never kept. The payload pages
    /// are fetched only where a row passes the filters on the other columns.
    /// Without an order, the read stops at `limit` states that pass all
    /// filters, thus a file of many states costs no more than the page
    /// needs. In an order, each state that passes is read, since the file is
    /// not sorted by time, but only `limit` of them are kept.
    pub async fn states(
        &self,
        file: &ObjectName,
        stats: FileStats,
        filter: &Arc<StatesFilter>,
        limit: NonZeroUsize,
        order: ScanOrder,
    ) -> Result<Vec<Numbered<StoredState>>, StatesReadError> {
        self.scan(file, stats, filter, limit, order)
            .await
            .map_err(|problem| StatesReadError::new(file, problem))
    }

    async fn read(
        &self,
        file: &ObjectName,
        stats: FileStats,
        tree: TreeId,
    ) -> Result<Vec<Numbered<StateRow>>, StatesFileProblem> {
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
        order: ScanOrder,
    ) -> Result<Vec<Numbered<StoredState>>, StatesFileProblem> {
        let open = self.open(file, stats).await?;
        // The columns are checked before any row is read, thus a file of
        // another layout is a problem whether or not a row passes.
        let schema = Arc::clone(open.metadata().schema());
        StatesBatch::try_from(&RecordBatch::new_empty(schema))?;
        let untested = Arc::new(AtomicUsize::new(0));
        let row_filter = filter.row_filter(open.metadata(), &untested);
        let mut batches = match order {
            ScanOrder::InFile { after } => open.filter(row_filter, Some(limit), after)?,
            ScanOrder::ByTime { .. } => open.filter(row_filter, None, None)?,
        };
        let mut states = Kept::new(limit, order);
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
        Ok(states.into_states())
    }

    async fn open(&self, file: &ObjectName, stats: FileStats) -> Result<OpenFile, FileProblem> {
        let store = Arc::clone(&self.store);
        OpenFile::open(store, file, stats, self.config.tail_bytes.get()).await
    }
}

/// The states that a scan keeps: all, in file order, or the first by time,
/// in a heap of the size of the page.
enum Kept {
    InFileOrder(Vec<Numbered<StoredState>>),
    ByTime {
        order: TsOrder,
        after: Option<FileKey>,
        states: Smallest<FileKey, Numbered<StoredState>>,
    },
}

impl Kept {
    fn new(limit: NonZeroUsize, order: ScanOrder) -> Self {
        match order {
            ScanOrder::InFile { .. } => Kept::InFileOrder(Vec::new()),
            ScanOrder::ByTime { order, after } => Kept::ByTime {
                order,
                after,
                states: Smallest::new(limit),
            },
        }
    }

    fn extend(&mut self, read: Vec<Numbered<StoredState>>) {
        match self {
            Kept::InFileOrder(states) => states.extend(read),
            Kept::ByTime {
                order,
                after,
                states,
            } => {
                for state in read {
                    let key = FileKey {
                        time: order.key(state.row.time),
                        row: state.number,
                    };
                    // A state that a page gave would take the place of one
                    // that the next page needs.
                    if after.is_none_or(|after| key > after) {
                        states.offer(key, state);
                    }
                }
            }
        }
    }

    fn into_states(self) -> Vec<Numbered<StoredState>> {
        match self {
            Kept::InFileOrder(states) => states,
            Kept::ByTime { states, .. } => states.into_sorted(),
        }
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
