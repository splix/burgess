//! Finding every row of a tree, or of one flow and the flows below it,
//! across the days they were written on.
//!
//! A tree id starts with the time its root was minted, and the writer files
//! rows by the day it flushes them, which is normally that day or later. So
//! a lookup starts on the id's day and walks forward. Each day costs at most
//! one manifest request, whose bounds rule out the files that cannot hold
//! the tree, and only the others are opened.

mod config;
mod error;
mod found;
mod lookup;
mod subtree;
mod walk;

pub use config::TreeLookupConfig;
pub use error::TreeLookupError;
pub use found::FoundTree;
pub use lookup::TreeLookup;

#[cfg(test)]
mod tests;
