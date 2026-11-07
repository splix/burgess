//! The states file: states and splits, sorted by tree, subflow and time.

use std::sync::Arc;

use arrow_array::builder::{MapBuilder, MapFieldNames, StringBuilder};
use arrow_array::{ArrayRef, BinaryArray, StringArray, UInt32Array};
use arrow_schema::{ArrowError, DataType, Field, Fields};
use burgess_core::storage::StatesColumn;

use super::columns::{Column, tree_type, tree_values, ts_type, ts_values};
use super::table::Table;
use crate::batch::{StateEntry, StateRow};

impl Table for StateRow {
    fn columns() -> Vec<Column<Self>> {
        vec![
            Column::required(StatesColumn::Tree.name(), tree_type(), |rows: &[Self]| {
                tree_values(rows.iter().map(|row| row.tree))
            })
            .sort_key(),
            Column::required(
                StatesColumn::Subflow.name(),
                DataType::UInt32,
                |rows: &[Self]| {
                    Ok(Arc::new(UInt32Array::from_iter_values(
                        rows.iter().map(|row| u32::from(row.subflow)),
                    )))
                },
            )
            .sort_key(),
            Column::required(StatesColumn::Ts.name(), ts_type(), |rows: &[Self]| {
                Ok(ts_values(rows.iter().map(|row| row.time)))
            })
            .sort_key(),
            Column::required(
                StatesColumn::Kind.name(),
                DataType::Utf8,
                |rows: &[Self]| {
                    Ok(Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.entry.kind().name()),
                    )))
                },
            ),
            Column::required(
                StatesColumn::Service.name(),
                DataType::Utf8,
                |rows: &[Self]| {
                    Ok(Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.service.as_str()),
                    )))
                },
            ),
            Column::required(
                StatesColumn::Category.name(),
                DataType::Utf8,
                |rows: &[Self]| {
                    Ok(Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.category.as_str()),
                    )))
                },
            ),
            Column::nullable(
                StatesColumn::ChildSubflow.name(),
                DataType::UInt32,
                |rows: &[Self]| {
                    Ok(Arc::new(UInt32Array::from_iter(rows.iter().map(
                        |row| match row.entry {
                            StateEntry::Split { child } => Some(u32::from(child)),
                            StateEntry::State { .. } => None,
                        },
                    ))))
                },
            ),
            Column::required(StatesColumn::Meta.name(), meta_type(), meta_values),
            Column::nullable(
                StatesColumn::PayloadType.name(),
                DataType::Utf8,
                |rows: &[Self]| {
                    Ok(Arc::new(StringArray::from_iter(rows.iter().map(
                        |row| match &row.entry {
                            StateEntry::State { payload_type, .. } => Some(payload_type.name()),
                            StateEntry::Split { .. } => None,
                        },
                    ))))
                },
            ),
            Column::nullable(
                StatesColumn::Payload.name(),
                DataType::Binary,
                |rows: &[Self]| {
                    Ok(Arc::new(BinaryArray::from_iter(rows.iter().map(
                        |row| match &row.entry {
                            StateEntry::State { payload, .. } => Some(payload.as_slice()),
                            StateEntry::Split { .. } => None,
                        },
                    ))))
                },
            )
            .blob(),
        ]
    }

    fn approximate_bytes(&self) -> usize {
        let entry = match &self.entry {
            StateEntry::State { meta, payload, .. } => {
                payload.len()
                    + meta
                        .iter()
                        .map(|(key, value)| key.len() + value.len())
                        .sum::<usize>()
            }
            StateEntry::Split { .. } => 0,
        };
        self.service.len() + self.category.len() + entry
    }
}

/// Names follow the Parquet convention for maps, which other readers such as
/// DuckDB and Spark expect.
fn meta_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "key_value".into(),
        key: "key".into(),
        value: "value".into(),
    }
}

fn meta_value_field() -> Field {
    Field::new(meta_field_names().value, DataType::Utf8, false)
}

fn meta_type() -> DataType {
    let names = meta_field_names();
    let entries = Fields::from(vec![
        Field::new(names.key, DataType::Utf8, false),
        meta_value_field(),
    ]);
    DataType::Map(
        Arc::new(Field::new(names.entry, DataType::Struct(entries), false)),
        false,
    )
}

/// A split carries no labels, so its map is empty rather than null.
fn meta_values(rows: &[StateRow]) -> Result<ArrayRef, ArrowError> {
    let mut builder = MapBuilder::new(
        Some(meta_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Arc::new(meta_value_field()));
    for row in rows {
        if let StateEntry::State { meta, .. } = &row.entry {
            for (key, value) in meta {
                builder.keys().append_value(key);
                builder.values().append_value(value);
            }
        }
        builder.append(true)?;
    }
    Ok(Arc::new(builder.finish()))
}
