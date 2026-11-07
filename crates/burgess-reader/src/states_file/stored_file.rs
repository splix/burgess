//! A Parquet file in the object store, read by byte ranges.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

/// A file whose size the manifest already gave, so every request is a
/// bounded range and the footer needs no HEAD or suffix request first.
pub(super) struct StoredFile {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
    /// Bytes fetched from the end in the first request, hoping they cover
    /// the footer and the page index.
    tail_bytes: usize,
}

impl StoredFile {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        path: Path,
        size: u64,
        tail_bytes: usize,
    ) -> Self {
        StoredFile {
            store,
            path,
            size,
            tail_bytes,
        }
    }
}

impl AsyncFileReader for StoredFile {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(store_error)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(store_error)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let size = self.size;
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(options.map(|options| options.metadata_options().clone()))
                .with_column_index_policy(options.map_or(PageIndexPolicy::Skip, |options| {
                    options.column_index_policy()
                }))
                .with_offset_index_policy(options.map_or(PageIndexPolicy::Skip, |options| {
                    options.offset_index_policy()
                }))
                .with_prefetch_hint(Some(self.tail_bytes))
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

fn store_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}
