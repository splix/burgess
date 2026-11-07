//! A file layout: schema and encoding settings derived from a column list.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use burgess_core::storage::{SCHEMA_VERSION, SCHEMA_VERSION_KEY};
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use super::columns::{Column, Role};
use super::config::ParquetConfig;
use super::table::Table;
use super::writer::{FileError, ParquetFile};

/// Approximate bytes of rows converted to Arrow at a time, which bounds the
/// memory the conversion needs on top of the batch itself.
const BYTES_PER_WRITE: usize = 64 * 1024 * 1024;

/// Everything needed to encode rows of one type, prepared once.
pub(super) struct FileLayout<R> {
    columns: Vec<Column<R>>,
    schema: SchemaRef,
    properties: WriterProperties,
}

impl<R: Table> FileLayout<R> {
    pub(super) fn new(config: &ParquetConfig, compression: Compression) -> Result<Self, FileError> {
        let columns = R::columns();
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| Field::new(column.name, column.data_type.clone(), column.nullable))
                .collect::<Vec<_>>(),
        ));
        let properties = properties(&columns, &schema, config, compression)?;
        Ok(FileLayout {
            columns,
            schema,
            properties,
        })
    }

    pub(super) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Encodes `rows`, in the given order, as one file.
    pub(super) fn encode(&self, rows: &[R]) -> Result<ParquetFile, FileError> {
        let mut writer =
            ArrowWriter::try_new(Vec::new(), self.schema(), Some(self.properties.clone()))?;
        let mut start = 0;
        let mut bytes = 0;
        for (index, row) in rows.iter().enumerate() {
            bytes += row.approximate_bytes();
            if bytes >= BYTES_PER_WRITE {
                writer.write(&self.record_batch(&rows[start..=index])?)?;
                start = index + 1;
                bytes = 0;
            }
        }
        if start < rows.len() {
            writer.write(&self.record_batch(&rows[start..])?)?;
        }
        Ok(ParquetFile {
            bytes: writer.into_inner()?,
            rows: rows.len(),
        })
    }

    /// The rows as Arrow columns, in the given order.
    pub(super) fn record_batch(&self, rows: &[R]) -> Result<RecordBatch, FileError> {
        let arrays = self
            .columns
            .iter()
            .map(|column| (column.values)(rows))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(self.schema(), arrays)?)
    }
}

fn properties<R>(
    columns: &[Column<R>],
    schema: &SchemaRef,
    config: &ParquetConfig,
    compression: Compression,
) -> Result<WriterProperties, FileError> {
    let leaves = ArrowSchemaConverter::new().convert(schema)?;
    let leaf_index = |name: &str| {
        leaves
            .columns()
            .iter()
            .position(|leaf| leaf.path().string() == name)
    };

    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(config.max_row_group_rows.get()))
        .set_data_page_size_limit(config.data_page_bytes.get())
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_key_value_metadata(Some(vec![
            KeyValue::new(SCHEMA_VERSION_KEY.into(), SCHEMA_VERSION.to_string()),
            KeyValue::new(
                "burgess.writer".into(),
                concat!("burgess-writer ", env!("CARGO_PKG_VERSION")).to_string(),
            ),
        ]));
    let mut sorting_columns = Vec::new();
    for column in columns {
        if column.role == Role::Regular {
            continue;
        }
        // Per-column settings address a single leaf, so only a flat column
        // can be a sort key or a blob.
        let index = leaf_index(column.name)
            .filter(|_| !column.data_type.is_nested())
            .ok_or_else(|| {
                ParquetError::General(format!("{} must be a flat column", column.name))
            })?;
        let path = ColumnPath::from(column.name);
        builder = match column.role {
            Role::SortKey => {
                sorting_columns.push(SortingColumn {
                    column_idx: index as i32,
                    descending: false,
                    nulls_first: false,
                });
                builder.set_column_statistics_enabled(path, EnabledStatistics::Page)
            }
            Role::Blob => builder
                .set_column_statistics_enabled(path.clone(), EnabledStatistics::None)
                .set_column_dictionary_enabled(path, false),
            Role::Regular => builder,
        };
    }
    Ok(builder.set_sorting_columns(Some(sorting_columns)).build())
}
