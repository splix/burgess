//! Test doubles shared by the reader's tests.

use std::fmt;
use std::ops::Range;
use std::sync::Mutex;

use async_trait::async_trait;
use burgess_core::storage::Day;
use bytes::Bytes;
use futures::stream::BoxStream;
use jiff::civil::Date;
use jiff::{Span, Timestamp};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

/// An in-memory store that remembers every read it was asked for.
///
/// Ranges are recorded as the reader asks for them. The store's own handling
/// of a multi-range request merges nearby ranges, which would hide whether
/// the reader asked for a page it did not need.
#[derive(Debug, Default)]
pub(crate) struct RecordingStore {
    pub(crate) inner: InMemory,
    requests: Mutex<Vec<Request>>,
}

/// One read request.
#[derive(Debug, Clone)]
pub(crate) struct Request {
    /// The object read.
    pub(crate) path: String,
    /// The byte ranges asked for, or `None` for the whole object.
    pub(crate) ranges: Option<Vec<Range<u64>>>,
}

impl RecordingStore {
    /// Every request so far.
    pub(crate) fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    /// The objects read so far, one entry per request.
    pub(crate) fn paths(&self) -> Vec<String> {
        self.requests()
            .into_iter()
            .map(|request| request.path)
            .collect()
    }

    /// Every range asked for so far. Panics if an object was read whole,
    /// since then the ranges do not tell what was fetched.
    pub(crate) fn ranges(&self) -> Vec<Range<u64>> {
        self.requests()
            .into_iter()
            .flat_map(|request| {
                request
                    .ranges
                    .unwrap_or_else(|| panic!("{} was read whole", request.path))
            })
            .collect()
    }

    fn record(&self, location: &Path, ranges: Option<Vec<Range<u64>>>) {
        self.requests.lock().unwrap().push(Request {
            path: location.to_string(),
            ranges,
        });
    }
}

impl fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingStore({})", self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        assert!(!options.head, "the reader never sends HEAD requests");
        match &options.range {
            None => self.record(location, None),
            Some(GetRange::Bounded(range)) => self.record(location, Some(vec![range.clone()])),
            other => panic!("the reader must ask for bounded ranges, got {other:?}"),
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.record(location, Some(ranges.to_vec()));
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// The day `days` before today by the host clock, which the reader also
/// uses for retention, so test days stay within it whenever the tests run.
pub(crate) fn days_ago(days: i64) -> Day {
    let today = Date::from(Day::utc(Timestamp::now()).unwrap());
    Day::try_from(today.checked_sub(Span::new().days(days)).unwrap()).unwrap()
}
