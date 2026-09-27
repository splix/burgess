//! Writing a batch as the two Parquet files lookups read.
//!
//! The query service keeps no database: it answers every lookup by reading
//! these files straight from the object store. The layout is chosen for that.
//! Rows are sorted on the lookup key, and the sort columns carry page-level
//! statistics, so a lookup reads only the pages that can hold its key.

mod columns;
mod config;
mod layout;
mod refs;
mod states;
mod table;
mod writer;

pub use config::ParquetConfig;
pub use writer::{BatchFiles, FileError, FileWriter, ParquetFile};

#[cfg(test)]
mod tests;
