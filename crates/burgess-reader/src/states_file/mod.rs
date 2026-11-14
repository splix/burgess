//! Reading one tree, or the states that pass a filter, out of one states
//! file.
//!
//! A lookup pays per round trip to the object store, not per byte, and most
//! files it opens hold nothing of the tree it wants. So a read fetches the
//! file's tail first, which holds the footer and the page index, and decides
//! from the statistics on `tree` alone which pages can hold the tree. A tree
//! the statistics rule out costs no data page, and only that one request
//! when the tail is long enough to hold the footer and the page index.
//!
//! A scan by service, category and time cannot use the sort order. It reads
//! the small columns of each row first, and the payload only for the rows
//! that pass.

mod decode;
mod error;
mod filter;
mod reader;
mod scan_order;
mod stored_state;

pub use decode::InvalidRow;
pub use error::{StatesFileProblem, StatesReadError};
pub use filter::StatesFilter;
pub use reader::StatesFileReader;
pub use scan_order::{FileKey, ScanOrder};
pub use stored_state::StoredState;

#[cfg(test)]
mod tests;
