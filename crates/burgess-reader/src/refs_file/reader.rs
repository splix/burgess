//! Reading the references of one outside id from one refs file.

use std::sync::Arc;

use burgess_core::flow::TreeId;
use burgess_core::storage::{FileStats, ObjectName, RefRow, RefsColumn};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::basic::Type as PhysicalType;

use super::decode::RefsBatch;
use super::error::{RefsFileProblem, RefsReadError};
use super::key::RefKey;
use crate::parquet_file::{ColumnKey, FileProblem, FileReadConfig, leaf_column, open_selected};

/// Reads the references of outside ids out of refs files, one file at a
/// time.
///
/// As the states reader, it gets the size of the file from the manifest,
/// thus no file is listed or probed with a HEAD.
pub struct RefsFileReader {
    store: Arc<dyn ObjectStore>,
    config: FileReadConfig,
}

impl RefsFileReader {
    /// A reader of the refs files in `store`.
    pub fn new(store: Arc<dyn ObjectStore>, config: FileReadConfig) -> Self {
        RefsFileReader { store, config }
    }

    /// All references to `key` in the refs file `file`, which the manifest
    /// lists with `stats`, in the order the file stores them.
    ///
    /// When the bounds of the key columns rule out the key, the read gets
    /// only the tail of the file.
    pub async fn refs(
        &self,
        file: &ObjectName,
        stats: FileStats,
        key: &RefKey,
    ) -> Result<Vec<RefRow>, RefsReadError> {
        self.read(file, stats, key)
            .await
            .map_err(|problem| RefsReadError::new(file, problem))
    }

    async fn read(
        &self,
        file: &ObjectName,
        stats: FileStats,
        key: &RefKey,
    ) -> Result<Vec<RefRow>, RefsFileProblem> {
        let keys = |metadata: &ArrowReaderMetadata| {
            // Not a key column, but a tree column of another width would be
            // read as other trees.
            leaf_column(
                metadata,
                RefsColumn::Tree.name(),
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
                Some(TreeId::LEN),
            )?;
            Ok(vec![
                ColumnKey {
                    column: string_column(metadata, RefsColumn::RefType)?,
                    value: key.ref_type().as_bytes(),
                },
                ColumnKey {
                    column: string_column(metadata, RefsColumn::DomainId)?,
                    value: key.domain_id().as_bytes(),
                },
            ])
        };
        let store = Arc::clone(&self.store);
        let tail_bytes = self.config.refs_tail_bytes.get();
        let Some(mut batches) = open_selected(store, file, stats, tail_bytes, keys).await? else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            rows.extend(RefsBatch::try_from(&batch)?.rows_of(key)?);
        }
        Ok(rows)
    }
}

fn string_column(metadata: &ArrowReaderMetadata, name: RefsColumn) -> Result<usize, FileProblem> {
    leaf_column(metadata, name.name(), PhysicalType::BYTE_ARRAY, None)
}
