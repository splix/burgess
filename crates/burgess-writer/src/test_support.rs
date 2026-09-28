//! Test doubles shared by the writer's tests: a store that records and can
//! fail writes, and a clock set by hand.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use jiff::Timestamp;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

use crate::flush::Clock;

/// An in-memory store that remembers the order of writes and can fail the
/// write to one object.
#[derive(Debug, Default)]
pub(crate) struct RecordingStore {
    pub(crate) inner: InMemory,
    writes: Mutex<Vec<String>>,
    reads: Mutex<Vec<String>>,
    /// Writes to a path ending with this fail, after letting the given
    /// number of them through.
    fail_writes_to: Mutex<Option<(String, usize)>>,
}

impl RecordingStore {
    pub(crate) fn writes(&self) -> Vec<String> {
        self.writes.lock().unwrap().clone()
    }

    pub(crate) fn reads(&self) -> Vec<String> {
        self.reads.lock().unwrap().clone()
    }

    pub(crate) fn fail_writes_to(&self, suffix: Option<&str>) {
        *self.fail_writes_to.lock().unwrap() = suffix.map(|suffix| (suffix.to_string(), 0));
    }

    /// Fails writes to paths ending with `suffix` once `skip` of them went
    /// through: the checkpoint is written twice per flush, and a test may want
    /// only the second write to fail.
    pub(crate) fn fail_writes_to_after(&self, suffix: &str, skip: usize) {
        *self.fail_writes_to.lock().unwrap() = Some((suffix.to_string(), skip));
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
        if let Some((suffix, skip)) = self.fail_writes_to.lock().unwrap().as_mut()
            && location.as_ref().ends_with(suffix.as_str())
        {
            if *skip == 0 {
                return Err(object_store::Error::Generic {
                    store: "RecordingStore",
                    source: "injected failure".into(),
                });
            }
            *skip -= 1;
        }
        self.writes.lock().unwrap().push(location.to_string());
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
        self.reads.lock().unwrap().push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
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

/// A clock the test sets by hand.
#[derive(Clone)]
pub(crate) struct TestClock(Arc<Mutex<Timestamp>>);

impl TestClock {
    pub(crate) fn at(time: &str) -> Self {
        TestClock(Arc::new(Mutex::new(time.parse().unwrap())))
    }

    pub(crate) fn set(&self, time: &str) {
        *self.0.lock().unwrap() = time.parse().unwrap();
    }
}

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        *self.0.lock().unwrap()
    }
}
