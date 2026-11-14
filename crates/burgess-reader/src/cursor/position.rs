//! The position of a scan, as a cursor carries it.

use burgess_core::storage::Day;
use burgess_core::wire::UnixMillis;

use super::error::CursorError;
use super::proto;
use crate::manifests::FlushGroup;
use crate::parquet_file::Numbered;

/// The last row that an answer gave. The next page starts after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// In an answer without an order. Rows come file by file, thus the row
    /// in its file is sufficient.
    Unordered(RowPosition),
    /// In an answer ordered by the state time. The rows of many files mix,
    /// thus the time is first, and the file row only orders the states of
    /// the same time.
    Ordered {
        /// The time of the row by the producer clock.
        ts: UnixMillis,
        /// The row in its file.
        row: RowPosition,
    },
}

impl Position {
    /// The stored row of the position.
    pub fn row(&self) -> RowPosition {
        match *self {
            Position::Unordered(row) | Position::Ordered { row, .. } => row,
        }
    }
}

/// A row of a stored file.
///
/// A stored file is named by its day and its flush time, thus the cursor
/// keeps both. Then the next page finds the manifest of the file without a
/// scan of the days, and does not depend on how the writer picks the day.
///
/// Positions compare by day, by flush, then by row, which is the order of
/// the stored rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowPosition {
    /// The day folder of the file.
    pub day: Day,
    /// The flush time that names the file.
    pub nnnn: UnixMillis,
    /// The number of the row in its file, from 0. It is the number of the
    /// row as stored, thus the same on each page.
    pub index: u64,
}

/// A row of an answer, and where it is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Positioned<T> {
    /// Where the row is, as a cursor after it tells.
    pub position: RowPosition,
    /// The row.
    pub row: T,
}

impl RowPosition {
    /// The first row of the files of `group`.
    pub(crate) fn first_in(group: &FlushGroup) -> Self {
        RowPosition {
            day: group.day(),
            nnnn: group.nnnn(),
            index: 0,
        }
    }
}

impl<T> Positioned<T> {
    /// `row`, read from a file of `group`.
    pub(crate) fn in_flush(group: &FlushGroup, row: Numbered<T>) -> Self {
        Positioned {
            position: RowPosition {
                index: row.number,
                ..RowPosition::first_in(group)
            },
            row: row.row,
        }
    }
}

impl From<Position> for proto::cursor::Position {
    fn from(position: Position) -> Self {
        match position {
            Position::Unordered(row) => proto::cursor::Position::Unordered(row.into()),
            Position::Ordered { ts, row } => {
                proto::cursor::Position::Ordered(proto::OrderedPosition {
                    ts_ms: ts.into(),
                    row: Some(row.into()),
                })
            }
        }
    }
}

impl TryFrom<proto::cursor::Position> for Position {
    type Error = CursorError;

    fn try_from(position: proto::cursor::Position) -> Result<Self, Self::Error> {
        match position {
            proto::cursor::Position::Unordered(row) => Ok(Position::Unordered(row.try_into()?)),
            proto::cursor::Position::Ordered(ordered) => Ok(Position::Ordered {
                ts: ordered.ts_ms.into(),
                row: ordered.row.ok_or(CursorError::NoPosition)?.try_into()?,
            }),
        }
    }
}

impl From<RowPosition> for proto::RowPosition {
    fn from(row: RowPosition) -> Self {
        proto::RowPosition {
            day: row.day.to_string(),
            nnnn: row.nnnn.into(),
            index: row.index,
        }
    }
}

impl TryFrom<proto::RowPosition> for RowPosition {
    type Error = CursorError;

    fn try_from(row: proto::RowPosition) -> Result<Self, Self::Error> {
        Ok(RowPosition {
            day: row.day.parse()?,
            nnnn: row.nnnn.into(),
            index: row.index,
        })
    }
}
