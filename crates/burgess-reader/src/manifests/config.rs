//! How long the cache trusts what it fetched.

use std::num::NonZeroU32;
use std::time::Duration;

/// Settings of a [`ManifestCache`](super::ManifestCache).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestCacheConfig {
    /// How long a manifest that may still change, or the absence of one, is
    /// used before it is fetched again. Zero fetches it for every lookup.
    pub unsealed_ttl: Duration,
    /// Days the deployment keeps, counted back from today. Earlier days are
    /// empty to the reader. Never set it shorter than the purge's window:
    /// the reader would hide days still stored.
    pub retention_days: NonZeroU32,
}

impl Default for ManifestCacheConfig {
    /// A TTL of 5 seconds and a retention of 31 days.
    ///
    /// The TTL is provisional. Lookups on the current day are not expected
    /// to be complete, since the latest flush may be minutes away, so a few
    /// seconds of staleness costs nothing, while it spares the store a
    /// request per lookup during an investigation that runs many lookups in
    /// a row. The retention matches the month deployments keep today.
    fn default() -> Self {
        ManifestCacheConfig {
            unsealed_ttl: Duration::from_secs(5),
            retention_days: NonZeroU32::new(31).expect("not zero"),
        }
    }
}
