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
/// overwrites a file readers may already use. The manifest and the bloom
/// filters have one fixed name per day, without the `.parquet` extension of
/// the data files. The checkpoint sits outside the days, one per writer.
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
    /// `YYYYMMDD/manifest`
    Manifest {
        /// The day it lists.
        day: Day,
    },
    /// `YYYYMMDD/refs.bloom`, the references filter of the day.
    RefsBloom {
        /// The day of the filter.
        day: Day,
    },
    /// `YYYYMMDD/trees.bloom`, the trees filter of the day.
    TreesBloom {
        /// The day of the filter.
        day: Day,
    },
    /// `checkpoint/<writer_id>.json`
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
            ObjectName::Manifest { day } => write!(f, "{day}/manifest"),
            ObjectName::RefsBloom { day } => write!(f, "{day}/refs.bloom"),
            ObjectName::TreesBloom { day } => write!(f, "{day}/trees.bloom"),
            ObjectName::Checkpoint { writer_id } => write!(f, "checkpoint/{writer_id}.json"),
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
            "20260927/manifest"
        );
        assert_eq!(
            ObjectName::RefsBloom { day: day() }.to_string(),
            "20260927/refs.bloom"
        );
        assert_eq!(
            ObjectName::TreesBloom { day: day() }.to_string(),
            "20260927/trees.bloom"
        );
        assert_eq!(
            ObjectName::Checkpoint {
                writer_id: "writer-1".parse().unwrap()
            }
            .to_string(),
            "checkpoint/writer-1.json"
        );
    }
}
