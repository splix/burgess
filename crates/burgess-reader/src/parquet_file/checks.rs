//! Checks that a file is the one the manifest lists, in a known layout.

use burgess_core::storage::{FileStats, SCHEMA_VERSION, SCHEMA_VERSION_KEY};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaData;

use super::problem::FileProblem;

pub(crate) fn check_layout(metadata: &ParquetMetaData) -> Result<(), FileProblem> {
    let found = metadata
        .file_metadata()
        .key_value_metadata()
        .and_then(|entries| entries.iter().find(|entry| entry.key == SCHEMA_VERSION_KEY))
        .and_then(|entry| entry.value.clone());
    match found {
        Some(version) if version == SCHEMA_VERSION => Ok(()),
        found => Err(FileProblem::UnknownLayout { found }),
    }
}

pub(crate) fn check_row_count(
    metadata: &ParquetMetaData,
    stats: FileStats,
) -> Result<(), FileProblem> {
    let found = metadata.file_metadata().num_rows();
    if u64::try_from(found).is_ok_and(|found| found == stats.rows) {
        Ok(())
    } else {
        Err(FileProblem::RowCount {
            listed: stats.rows,
            found,
        })
    }
}

/// The leaf index of the column `name`, which the statistics are looked up
/// by. `length` is the width of a fixed length column.
///
/// The type is checked here, because a key column of another type compares
/// against no key, and each lookup would wrongly find the file empty.
pub(crate) fn leaf_column(
    metadata: &ArrowReaderMetadata,
    name: &'static str,
    physical_type: PhysicalType,
    length: Option<usize>,
) -> Result<usize, FileProblem> {
    metadata
        .parquet_schema()
        .columns()
        .iter()
        .position(|column| {
            column.path().string() == name
                && column.physical_type() == physical_type
                && length.is_none_or(|length| usize::try_from(column.type_length()) == Ok(length))
        })
        .ok_or(FileProblem::Column { column: name })
}
