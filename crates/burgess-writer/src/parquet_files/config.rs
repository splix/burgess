//! Tunable settings of the Parquet files.

use std::num::NonZeroUsize;

/// How the Parquet files are encoded.
///
/// Sizes decide how finely a lookup can skip data: smaller pages mean less
/// read per lookup but more index to read first. Their best values depend on
/// real traffic, so they are configuration with provisional defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetConfig {
    /// zstd compression level, 1 to 22.
    pub zstd_level: i32,
    /// Most rows in one row group.
    pub max_row_group_rows: NonZeroUsize,
    /// Size a data page is closed at, in bytes before compression.
    pub data_page_bytes: NonZeroUsize,
}

impl Default for ParquetConfig {
    /// zstd level 3; row group and page sizes are the `parquet` crate's own
    /// defaults (1 Mi rows, 1 MiB), provisional until measured.
    fn default() -> Self {
        ParquetConfig {
            zstd_level: 3,
            max_row_group_rows: NonZeroUsize::new(1024 * 1024).expect("not zero"),
            data_page_bytes: NonZeroUsize::new(1024 * 1024).expect("not zero"),
        }
    }
}
