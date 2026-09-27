//! Where each stored object lives.
//!
//! The writer puts objects at these names and the query service gets them
//! from the same names without listing the bucket, so the layout is defined
//! once, here.

use std::fmt;

use crate::wire::UnixMillis;

use super::day::Day;
use super::writer_id::WriterId;

/// The name of an object the writer stores.
///
/// Data files carry their flush time, so every flush gets new names and never
/// overwrites a file readers may already use. The manifest has one fixed name
/// per day; its `_` prefix keeps it out of `*.parquet` patterns. The
/// checkpoint sits outside the days, one per writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectName {
    /// `YYYYMMDD/NNNN-states.parquet`
    StatesFile {
        /// The day of the flush.
        day: Day,
        /// The flush time.
        nnnn: UnixMillis,
    },
    /// `YYYYMMDD/NNNN-refs.parquet`
    RefsFile {
        /// The day of the flush.
        day: Day,
        /// The flush time.
        nnnn: UnixMillis,
    },
    /// `YYYYMMDD/_manifest`
    Manifest {
        /// The day it lists.
        day: Day,
    },
    /// `_checkpoint/<writer_id>.json`
    Checkpoint {
        /// The writer it belongs to.
        writer_id: WriterId,
    },
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectName::StatesFile { day, nnnn } => {
                write!(f, "{day}/{}-states.parquet", u64::from(*nnnn))
            }
            ObjectName::RefsFile { day, nnnn } => {
                write!(f, "{day}/{}-refs.parquet", u64::from(*nnnn))
            }
            ObjectName::Manifest { day } => write!(f, "{day}/_manifest"),
            ObjectName::Checkpoint { writer_id } => write!(f, "_checkpoint/{writer_id}.json"),
        }
    }
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;

    use super::*;

    fn day() -> Day {
        Day::try_from(date(2026, 9, 27)).unwrap()
    }

    #[test]
    fn names_follow_the_layout() {
        let nnnn = UnixMillis::from(1_790_000_000_123);
        assert_eq!(
            ObjectName::StatesFile { day: day(), nnnn }.to_string(),
            "20260927/1790000000123-states.parquet"
        );
        assert_eq!(
            ObjectName::RefsFile { day: day(), nnnn }.to_string(),
            "20260927/1790000000123-refs.parquet"
        );
        assert_eq!(
            ObjectName::Manifest { day: day() }.to_string(),
            "20260927/_manifest"
        );
        assert_eq!(
            ObjectName::Checkpoint {
                writer_id: "writer-1".parse().unwrap()
            }
            .to_string(),
            "_checkpoint/writer-1.json"
        );
    }
}
