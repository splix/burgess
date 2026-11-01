//! Which data files exist, as the day manifests say.
//!
//! Listing a bucket is slow and costs a request per page, so readers never
//! list. Each day's manifest names every file a lookup may open, and a data
//! file it does not name does not exist for readers: it is what a flush left
//! behind when it failed before finishing.

mod cache;
mod config;
mod day_flushes;
mod error;

pub use cache::ManifestCache;
pub use config::ManifestCacheConfig;
pub use day_flushes::{DayFlushes, FlushGroup};
pub use error::ManifestReadError;
