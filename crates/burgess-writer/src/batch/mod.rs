//! Collecting records into batches, one batch per flush.
//!
//! Reads are rare and storage is cheap, so the writer does its sorting up
//! front: it holds records in memory for a while, then writes each batch as
//! files sorted the way lookups need them. A batch ends when it has been open
//! for the configured interval or has reached the configured size, whichever
//! comes first.

mod accumulator;
mod collect;
mod config;
mod rows;
mod skip;

pub use accumulator::Accumulator;
pub use burgess_core::storage::{StateEntry, StateRow};
pub use collect::SortedBatch;
pub use config::{BatchConfig, BatchConfigError};
pub use rows::RefRow;
