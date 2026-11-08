//! Turning what a lookup found into the pages the API returns.
//!
//! Kept apart from any one transport, so that every transport serving a
//! lookup returns the same rows, the same limits and the same problems.

mod limits;
mod page;
mod row;

pub use limits::{LimitError, PageLimits, PageLimitsError};

#[cfg(test)]
mod tests;
