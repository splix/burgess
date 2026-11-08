//! Reading one tree out of one states file.
//!
//! A lookup pays per round trip to the object store, not per byte, and most
//! files it opens hold nothing of the tree it wants. So a read fetches the
//! file's tail first, which holds the footer and the page index, and decides
//! from the statistics on `tree` alone which pages can hold the tree. A tree
//! the statistics rule out costs no data page, and only that one request
//! when the tail is long enough to hold the footer and the page index.

mod decode;
mod error;
mod reader;

pub use decode::InvalidRow;
pub use error::{StatesFileProblem, TreeReadError};
pub use reader::StatesFileReader;

#[cfg(test)]
mod tests;
