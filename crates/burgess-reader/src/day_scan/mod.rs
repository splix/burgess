//! Reading the files of a range of days, newest day first, until enough
//! rows are found.
//!
//! The refs lookup and the states lookup do not know on which days their
//! rows are, thus they read each day of a window. Most of the time is spent
//! on round trips to the store. Thus the manifests of the next days are read
//! ahead, and the files are read across days, so that the reads of one day
//! do not wait for the reads of the day before to complete.

mod config;
mod days;
mod gate;
mod problems;
mod scan;

pub use config::ScanConfig;
pub(crate) use days::{days_back, newest_day, oldest_day};
pub(crate) use gate::DayGate;
pub(crate) use problems::WalkProblems;
pub(crate) use scan::DayScan;
pub use scan::Found;
