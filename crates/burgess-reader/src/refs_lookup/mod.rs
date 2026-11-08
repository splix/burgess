//! Finding the references to an outside id, across the days kept.
//!
//! An outside id does not tell when it was used, thus a lookup walks the
//! days newest first, from the day after `until` to `since`, or over the
//! full retention when they are not given. For each day it reads the
//! manifest, then every refs file of the day that has rows. A later
//! version can probe a day bloom before the files, which removes most of
//! these reads. The walk stops when it has found more rows than the page
//! can hold.
//!
//! The writer stores a reference on the day it flushes it. A writer that
//! is late by more than a day stores a reference two or more days after
//! its time, and a lookup with `until` does not find it.

mod config;
mod error;
mod found;
mod lookup;
mod query;

pub use config::RefsLookupConfig;
pub use error::{RefsLookupError, UnboundedWalk};
pub use found::FoundRefs;
pub use lookup::RefsLookup;
pub use query::RefsQuery;

#[cfg(test)]
mod tests;
