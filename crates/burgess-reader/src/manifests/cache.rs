//! Day manifests kept in memory between lookups.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use burgess_core::storage::{Day, DayManifest, ObjectName};
use jiff::civil::Date;
use jiff::{Span, Timestamp};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::time::Instant;

use super::config::ManifestCacheConfig;
use super::day_flushes::DayFlushes;
use super::error::ManifestReadError;

/// Fetches day manifests, one request per day, and keeps them for later
/// lookups.
///
/// A sealed manifest never changes, so it is fetched once and kept. Every
/// other answer, including that a day has no manifest, is fetched again once
/// it is older than the unsealed TTL: the writer may add a flush to the day,
/// create its manifest, or seal it. A re-fetched manifest replaces the old
/// one whole, because a writer restart can also drop flushes from a day that
/// is not sealed yet.
///
/// Days older than the retention window are empty without asking the
/// store, and are dropped from memory, so the cache holds at most the
/// retention window: retention deletes a day's files, sealed or not.
///
/// Two lookups that miss the same day at once both fetch it. The answer
/// fetched later is kept, and a sealed one is never replaced.
pub struct ManifestCache {
    store: Arc<dyn ObjectStore>,
    config: ManifestCacheConfig,
    days: Mutex<HashMap<Day, Cached>>,
}

/// What the store said about a day, and when it was asked.
struct Cached {
    flushes: Option<Arc<DayFlushes>>,
    /// When the request started rather than finished, so the TTL also
    /// covers the time the store took to answer.
    fetched_at: Instant,
}

impl Cached {
    fn is_sealed(&self) -> bool {
        self.flushes
            .as_ref()
            .is_some_and(|flushes| flushes.is_sealed())
    }
}

impl ManifestCache {
    /// A cache over `store`.
    pub fn new(store: Arc<dyn ObjectStore>, config: ManifestCacheConfig) -> Self {
        ManifestCache {
            store,
            config,
            days: Mutex::new(HashMap::new()),
        }
    }

    /// The flushes of `day`, or `None` if the writer stored nothing that day:
    /// days without flushes never get a manifest, and expired days lose
    /// theirs.
    pub async fn day(&self, day: Day) -> Result<Option<Arc<DayFlushes>>, ManifestReadError> {
        if self.oldest_kept().is_some_and(|oldest| day < oldest) {
            return Ok(None);
        }
        if let Some(flushes) = self.cached(day) {
            return Ok(flushes);
        }
        let fetched_at = Instant::now();
        let flushes = self.fetch(day).await?.map(Arc::new);
        Ok(self.keep(
            day,
            Cached {
                flushes,
                fetched_at,
            },
        ))
    }

    /// The cached answer for `day`, if it is still good.
    fn cached(&self, day: Day) -> Option<Option<Arc<DayFlushes>>> {
        let days = self.lock();
        let cached = days.get(&day)?;
        (cached.is_sealed() || cached.fetched_at.elapsed() < self.config.unsealed_ttl)
            .then(|| cached.flushes.clone())
    }

    /// Caches `fetched` unless the cache already holds a sealed answer or one
    /// fetched later, and returns whichever answer it keeps. Drops days that
    /// expired meanwhile.
    fn keep(&self, day: Day, fetched: Cached) -> Option<Arc<DayFlushes>> {
        let oldest = self.oldest_kept();
        let mut days = self.lock();
        if let Some(oldest) = oldest {
            days.retain(|kept, _| *kept >= oldest);
        }
        match days.get(&day) {
            Some(known) if known.is_sealed() || known.fetched_at > fetched.fetched_at => {
                known.flushes.clone()
            }
            _ => {
                let flushes = fetched.flushes.clone();
                days.insert(day, fetched);
                flushes
            }
        }
    }

    /// The oldest day still within retention, counting back from today by
    /// the host's clock. Earlier days are empty to this cache. `None` when
    /// that day is before the earliest day a manifest can name, which leaves
    /// every day in.
    pub fn oldest_kept(&self) -> Option<Day> {
        let today = Date::from(Day::utc(Timestamp::now()).ok()?);
        let retention = Span::new().days(i64::from(self.config.retention_days.get()));
        Day::try_from(today.checked_sub(retention).ok()?).ok()
    }

    async fn fetch(&self, day: Day) -> Result<Option<DayFlushes>, ManifestReadError> {
        let path = Path::from(ObjectName::Manifest { day }.to_string());
        let stored = match self.store.get(&path).await {
            Ok(stored) => stored,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(source) => return Err(ManifestReadError::Store { day, source }),
        };
        let bytes = stored
            .bytes()
            .await
            .map_err(|source| ManifestReadError::Store { day, source })?;
        let manifest = DayManifest::decode(&bytes)
            .map_err(|source| ManifestReadError::Invalid { day, source })?;
        if manifest.day != day {
            return Err(ManifestReadError::WrongDay {
                day,
                found: manifest.day,
            });
        }
        Ok(Some(manifest.into()))
    }

    /// Every change to the map is a single insert or removal, so a thread
    /// that panicked while holding the lock left it consistent.
    fn lock(&self) -> MutexGuard<'_, HashMap<Day, Cached>> {
        self.days.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
