//! Finding the states of a service and category in a time window, across
//! the days of the window.
//!
//! The files are sorted by tree, thus no bound tells which files hold a
//! service. A lookup reads each file of the window whose time bounds touch
//! it. This is slow on a long window, and this is accepted until a
//! measurement shows that it is a problem.
//!
//! The writer stores a state on the day it flushes it, which is normally the
//! day of its time or later. Thus a lookup also reads the day after `until`,
//! and the configured days before `since`. A writer that is late by more
//! than a day stores a state two or more days after its time, and a lookup
//! does not find it.

mod config;
mod error;
mod lookup;

pub use config::StatesLookupConfig;
pub use error::StatesLookupError;
pub use lookup::StatesLookup;

use crate::day_scan::Found;
use crate::states_file::StoredState;

/// The states a lookup found, and what it could not read.
pub type FoundStates = Found<StoredState, StatesLookupError>;

#[cfg(test)]
mod tests;
