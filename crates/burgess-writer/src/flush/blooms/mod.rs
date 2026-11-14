//! The bloom filters of the day that a flush goes to.
//!
//! Each flush writes the filters of its day, with its keys added, after its
//! data files and before the manifest. Thus a filter, whenever it is read,
//! holds the keys of each file listed by then, and a lookup that reads it
//! misses no row of a file listed before it read the filter.
//!
//! A filter is never made again from the data files. A day that has listed
//! flushes keeps its stored filters, with the keys of the new flushes added.
//! A filter that is gone or cannot be extended is not written again for its
//! day, and one that cannot be extended is deleted: a new one would not
//! hold the keys of the files listed before it, and a filter left as it is
//! would not hold the keys of the files listed after it. A lookup must read
//! each file of a day without a filter.

mod config;
mod keys;
mod sizing;

pub use config::{BloomConfig, BloomConfigError};

use std::sync::Arc;

use burgess_core::report::error_chain;
use burgess_core::storage::{BloomKey, Day, DayBloom, DayManifest, ObjectName, RefsKey, TreeKey};
use burgess_core::wire::UnixMillis;
use object_store::ObjectStoreExt;
use object_store::path::Path;

use self::keys::FlushKeys;
use self::sizing::new_day_size;
use super::clock::Clock;
use super::error::FlushError;
use super::flusher::{Flusher, get, on_blocking_thread};
use crate::batch::SortedBatch;

/// The filters of one day, each `None` when the day has none to write.
#[derive(Debug, Clone)]
pub(super) struct DayBlooms {
    day: Day,
    refs: Option<Kept<RefsKey>>,
    trees: Option<Kept<TreeKey>>,
}

/// A filter, and whether it differs from the stored one.
#[derive(Debug, Clone)]
struct Kept<K> {
    filter: DayBloom<K>,
    /// A new filter is not stored yet, thus it is written even without
    /// keys: the day has its filters from its first flush.
    changed: bool,
}

impl<K: BloomKey> Kept<K> {
    fn new(filter: DayBloom<K>) -> Self {
        Kept {
            filter,
            changed: true,
        }
    }

    fn stored(filter: DayBloom<K>) -> Self {
        Kept {
            filter,
            changed: false,
        }
    }

    /// Adds `keys`. A day busier than the days before goes over the
    /// capacity of its filter, whose size cannot change: the filter then
    /// says "maybe" more often, which the log tells one time.
    fn add(&mut self, keys: impl ExactSizeIterator<Item = K>) {
        if keys.len() == 0 {
            return;
        }
        let capacity = self.filter.capacity().get();
        let was_over = self.filter.insertions() > capacity;
        for key in keys {
            self.filter.insert(&key);
        }
        self.changed = true;
        if !was_over && self.filter.insertions() > capacity {
            tracing::warn!(
                day = %self.filter.day(),
                filter = K::KIND,
                capacity,
                "the bloom filter holds more keys than it was sized for; lookups read more files"
            );
        }
    }

    /// The object to store, if the filter differs from the stored one.
    fn object(&self) -> Option<(ObjectName, Vec<u8>)> {
        self.changed
            .then(|| (K::object(self.filter.day()), self.filter.encode()))
    }
}

impl DayBlooms {
    /// The filters with the keys of `batch`, which the flush `nnnn`
    /// stores, and the objects to store for them. On a blocking thread: a
    /// large batch has many keys to hash, and a filter megabytes to copy.
    async fn with_flush(
        mut self,
        batch: Arc<SortedBatch>,
        nnnn: UnixMillis,
    ) -> Result<(DayBlooms, Vec<(ObjectName, Vec<u8>)>), FlushError> {
        on_blocking_thread(move || {
            let keys = FlushKeys::of(&batch, nnnn);
            if let Some(refs) = &mut self.refs {
                refs.add(keys.refs.into_iter());
            }
            if let Some(trees) = &mut self.trees {
                trees.add(keys.trees.into_iter());
            }
            let objects = [
                self.refs.as_ref().and_then(Kept::object),
                self.trees.as_ref().and_then(Kept::object),
            ];
            (self, objects.into_iter().flatten().collect())
        })
        .await
    }

    /// The filters as stored.
    pub(super) fn stored(mut self) -> Self {
        if let Some(refs) = &mut self.refs {
            refs.changed = false;
        }
        if let Some(trees) = &mut self.trees {
            trees.changed = false;
        }
        self
    }
}

impl<C: Clock> Flusher<C> {
    /// The filters that a flush onto the day of `manifest` adds its keys
    /// to: new ones for a day without listed flushes, else the stored ones.
    ///
    /// The flusher keeps them as they were before the flush, and gives a
    /// copy. Thus a flush that fails, also after it stored its filters, is
    /// tried again on the filters without its keys, and the tries do not
    /// fill the filters with the keys of files that were never listed.
    pub(super) async fn day_blooms(
        &mut self,
        manifest: &DayManifest,
    ) -> Result<DayBlooms, FlushError> {
        let day = manifest.day;
        if let Some(blooms) = &self.blooms
            && blooms.day == day
        {
            return Ok(blooms.clone());
        }
        let blooms = if manifest.entries.is_empty() {
            DayBlooms {
                day,
                refs: Some(Kept::new(self.new_filter(day).await?)),
                trees: Some(Kept::new(self.new_filter(day).await?)),
            }
        } else {
            DayBlooms {
                day,
                refs: self.stored_filter(day).await?.map(Kept::stored),
                trees: self.stored_filter(day).await?.map(Kept::stored),
            }
        };
        self.blooms = Some(blooms.clone());
        Ok(blooms)
    }

    /// Adds the keys of `batch` to `blooms` and stores the filters that
    /// changed, both at the same time: only the manifest must come after
    /// them.
    pub(super) async fn store_blooms(
        &self,
        blooms: DayBlooms,
        batch: Arc<SortedBatch>,
        nnnn: UnixMillis,
    ) -> Result<DayBlooms, FlushError> {
        let (blooms, objects) = blooms.with_flush(batch, nnnn).await?;
        futures::future::try_join_all(
            objects
                .into_iter()
                .map(|(name, bytes)| async move { self.put(&name, bytes).await }),
        )
        .await?;
        Ok(blooms)
    }

    /// The stored filter of `day`, or `None` when it is gone or cannot be
    /// extended; one that cannot be extended is deleted. Only a failed
    /// request to the store is an error: the flush is tried again, and the
    /// filter is then read again.
    async fn stored_filter<K: BloomKey>(
        &self,
        day: Day,
    ) -> Result<Option<DayBloom<K>>, FlushError> {
        let Some(bytes) = get(self.store.as_ref(), &K::object(day)).await? else {
            tracing::warn!(
                %day,
                filter = K::KIND,
                "the day lists flushes but has no bloom filter; it gets none, and lookups read each file of the day"
            );
            return Ok(None);
        };
        let problem = match self.own_filter::<K>(day, &bytes) {
            Ok(filter) => return Ok(Some(filter)),
            Err(problem) => problem,
        };
        // A reader that can read the filter would trust it for the files
        // listed after it, which it would not hold.
        let path = Path::from(K::object(day).to_string());
        match self.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        tracing::warn!(
            %day,
            filter = K::KIND,
            %problem,
            "deleted the bloom filter of the day that this writer cannot extend; lookups read each file of the day"
        );
        Ok(None)
    }

    /// A new filter of `day`, sized by the busiest of the days before it in
    /// the window.
    async fn new_filter<K: BloomKey>(&self, day: Day) -> Result<DayBloom<K>, FlushError> {
        let mut busiest = 0;
        let mut earlier = day;
        for _ in 0..self.bloom_config.window_days.get() {
            let Some(previous) = earlier.previous() else {
                break;
            };
            earlier = previous;
            busiest = busiest.max(self.insertions_of::<K>(earlier).await?);
        }
        let size = new_day_size(&self.bloom_config, busiest);
        Ok(DayBloom::new(day, self.writer_id.clone(), size)?)
    }

    /// The keys that this writer's filter of `day` holds; 0 when it has
    /// none that can be read, as a day without flushes.
    async fn insertions_of<K: BloomKey>(&self, day: Day) -> Result<u64, FlushError> {
        let Some(bytes) = get(self.store.as_ref(), &K::object(day)).await? else {
            return Ok(0);
        };
        match self.own_filter::<K>(day, &bytes) {
            Ok(filter) => Ok(filter.insertions()),
            Err(problem) => {
                tracing::warn!(
                    %day,
                    filter = K::KIND,
                    %problem,
                    "a bloom filter of the days before cannot be read; it does not size the new day"
                );
                Ok(0)
            }
        }
    }

    /// The filter in `bytes`, if it is this writer's filter of `day`, or
    /// why not.
    fn own_filter<K: BloomKey>(&self, day: Day, bytes: &[u8]) -> Result<DayBloom<K>, String> {
        let filter = DayBloom::<K>::decode(bytes).map_err(|error| error_chain(&error))?;
        if filter.day() != day || *filter.writer_id() != self.writer_id {
            return Err(format!(
                "it is the filter of day {} by writer {}",
                filter.day(),
                filter.writer_id()
            ));
        }
        Ok(filter)
    }
}
