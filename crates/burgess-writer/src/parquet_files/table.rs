//! What a row type must describe to be written as a Parquet file.

use super::columns::Column;

/// A row type that is stored as one Parquet file per batch.
pub(super) trait Table: Sized {
    /// The file's columns, sort keys first and in sort order.
    fn columns() -> Vec<Column<Self>>;

    /// Rough size of the row once converted to columns, used to convert a
    /// batch in slices of bounded memory rather than all at once.
    fn approximate_bytes(&self) -> usize;
}
