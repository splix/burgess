//! The bloom filters of a day, as the writer stores them.
//!
//! A filter tells which days and files cannot hold a reference or a tree,
//! thus a lookup reads only the others. A filter answers only for the
//! files that were listed when it was read: the writer stores it before
//! the manifest that lists its files. Thus each lookup reads the filters
//! it uses, and never one read before it: an older filter would send it
//! away from files that were visible when it started.
//!
//! A day can have no usable filter. Most often it has no flush yet, or it
//! was stored before the writer made filters. The writer also gives none to
//! a day whose filter it lost, and the store or the object can fail. Then
//! the lookup reads each file of the day, as without filters. That hides no
//! row, thus it is logged for the operator, not told to the caller.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use burgess_core::report::error_chain;
use burgess_core::storage::{BloomKey, Day, DayBloom, MAX_BLOOM_BYTES, RefsBloom, TreesBloom};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

/// Room for the frame and the header before the bits of a filter, far over
/// what a header takes.
const HEADER_ROOM: u64 = 64 * 1024;

/// Reads the bloom filters of days.
#[derive(Clone)]
pub struct BloomReader {
    store: Arc<dyn ObjectStore>,
    /// The filters that this process told cannot be used. The writer does
    /// not repair one, thus each lookup over its day would tell it again.
    told: Arc<Mutex<HashSet<(Day, &'static str)>>>,
}

impl BloomReader {
    /// A reader of the filters in `store`.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        BloomReader {
            store,
            told: Arc::default(),
        }
    }

    /// The references filter of `day`, or `None` when it has none that can
    /// be used.
    pub(crate) async fn refs(&self, day: Day) -> Option<RefsBloom> {
        self.filter(day).await
    }

    /// The trees filter of `day`, or `None` when it has none that can be
    /// used.
    pub(crate) async fn trees(&self, day: Day) -> Option<TreesBloom> {
        self.filter(day).await
    }

    async fn filter<K: BloomKey + 'static>(&self, day: Day) -> Option<DayBloom<K>> {
        let problem = match self.read::<K>(day).await {
            Ok(Some(filter)) => return Some(filter),
            Ok(None) => {
                tracing::debug!(%day, filter = K::KIND, "the day has no bloom filter");
                return None;
            }
            Err(problem) => problem,
        };
        if self.first_told(day, K::KIND) {
            tracing::warn!(
                %day,
                filter = K::KIND,
                %problem,
                "the bloom filter of the day cannot be used; lookups read each file of the day"
            );
        }
        None
    }

    /// The filter of `day`, `None` when it is not stored, or why it cannot
    /// be used.
    async fn read<K: BloomKey + 'static>(&self, day: Day) -> Result<Option<DayBloom<K>>, String> {
        let path = Path::from(K::object(day).to_string());
        let stored = match self.store.get(&path).await {
            Ok(stored) => stored,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error_chain(&error)),
        };
        // A damaged object can be of any size, and the filter is held whole.
        let most = MAX_BLOOM_BYTES + HEADER_ROOM;
        if stored.meta.size > most {
            return Err(format!(
                "it has {} bytes, a filter has at most {most}",
                stored.meta.size
            ));
        }
        let bytes = stored.bytes().await.map_err(|error| error_chain(&error))?;
        let filter = decoded::<K>(bytes).await?;
        if filter.day() != day {
            return Err(format!("it is the filter of day {}", filter.day()));
        }
        Ok(Some(filter))
    }

    /// Whether this is the first time this process tells that the filter
    /// of `kind` of `day` cannot be used.
    fn first_told(&self, day: Day, kind: &'static str) -> bool {
        // Each change is one insert, thus a thread that panicked holding
        // the lock left the set whole.
        let mut told = self.told.lock().unwrap_or_else(PoisonError::into_inner);
        told.insert((day, kind))
    }
}

/// The filter in `bytes`, decoded on a blocking thread: it copies
/// megabytes, which would hold up the threads that serve requests.
async fn decoded<K: BloomKey + 'static>(bytes: Bytes) -> Result<DayBloom<K>, String> {
    match tokio::task::spawn_blocking(move || DayBloom::<K>::decode(&bytes)).await {
        Ok(decoded) => decoded.map_err(|error| error_chain(&error)),
        Err(failed) => match failed.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_) => Err("the service is shutting down".to_string()),
        },
    }
}
