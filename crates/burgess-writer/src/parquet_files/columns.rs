//! Columns described once: name, type, role and how their values are built.
//!
//! Everything else about a file (its schema, sort order and per-column
//! settings) is derived from the column list, so a column is named in exactly
//! one place and the pieces cannot drift apart.

use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{ArrayRef, TimestampMillisecondArray};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use burgess_core::flow::TreeId;
use burgess_core::wire::UnixMillis;

/// How a column takes part in lookups, which decides its encoding settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Role {
    /// Rows are sorted by it. Gets page-level statistics, so a lookup can
    /// skip pages that cannot hold its key.
    SortKey,
    /// Read or filtered after the sort key has narrowed the rows. Row-group
    /// statistics are enough.
    Regular,
    /// Large, mostly unique values. Statistics and dictionaries would cost
    /// work and space without ever helping a lookup.
    Blob,
}

/// Builds the values of one column from a slice of rows.
pub(super) type Values<R> = fn(&[R]) -> Result<ArrayRef, ArrowError>;

/// One column of a file.
pub(super) struct Column<R> {
    pub(super) name: &'static str,
    pub(super) data_type: DataType,
    pub(super) nullable: bool,
    pub(super) role: Role,
    pub(super) values: Values<R>,
}

impl<R> Column<R> {
    /// A column every row has a value in.
    pub(super) fn required(name: &'static str, data_type: DataType, values: Values<R>) -> Self {
        Column {
            name,
            data_type,
            nullable: false,
            role: Role::Regular,
            values,
        }
    }

    /// A column some rows leave empty.
    pub(super) fn nullable(name: &'static str, data_type: DataType, values: Values<R>) -> Self {
        Column {
            nullable: true,
            ..Self::required(name, data_type, values)
        }
    }

    /// Marks the column as one the rows are sorted by. Sort keys are listed
    /// first, in sort order.
    pub(super) fn sort_key(self) -> Self {
        Column {
            role: Role::SortKey,
            ..self
        }
    }

    /// Marks the column as holding large values not worth indexing.
    pub(super) fn blob(self) -> Self {
        Column {
            role: Role::Blob,
            ..self
        }
    }
}

/// Both files store the tree id as its 12 raw bytes, so min and max
/// statistics compare the same way the ids sort.
pub(super) fn tree_type() -> DataType {
    DataType::FixedSizeBinary(12)
}

pub(super) fn tree_values(
    trees: impl ExactSizeIterator<Item = TreeId>,
) -> Result<ArrayRef, ArrowError> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(trees.len(), 12);
    for tree in trees {
        builder.append_value(tree.as_bytes())?;
    }
    Ok(Arc::new(builder.finish()))
}

/// Unix milliseconds are instants, so the column is marked as UTC.
pub(super) fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

/// A time past the end of year 9999 is stored as that end: it is kept as
/// sent rather than rejected, but readers such as DuckDB cannot go further.
pub(super) fn ts_values(times: impl Iterator<Item = UnixMillis>) -> ArrayRef {
    let millis = times.map(|time| time.saturating_timestamp().as_millisecond());
    Arc::new(TimestampMillisecondArray::from_iter_values(millis).with_timezone("UTC"))
}
