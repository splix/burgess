//! The references file, sorted by reference type and outside id.

use std::sync::Arc;

use arrow_array::{StringArray, UInt32Array};
use arrow_schema::DataType;

use super::columns::{Column, tree_type, tree_values, ts_type, ts_values};
use super::table::Table;
use burgess_core::storage::RefsColumn;

use crate::batch::RefRow;

impl Table for RefRow {
    fn columns() -> Vec<Column<Self>> {
        vec![
            Column::required(
                RefsColumn::RefType.name(),
                DataType::Utf8,
                |rows: &[Self]| {
                    Ok(Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.ref_type.as_str()),
                    )))
                },
            )
            .sort_key(),
            Column::required(
                RefsColumn::DomainId.name(),
                DataType::Utf8,
                |rows: &[Self]| {
                    Ok(Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.domain_id.as_str()),
                    )))
                },
            )
            .sort_key(),
            Column::required(
                RefsColumn::Service.name(),
                DataType::Utf8,
                |rows: &[Self]| {
                    Ok(Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.service.as_str()),
                    )))
                },
            ),
            Column::nullable(
                RefsColumn::Category.name(),
                DataType::Utf8,
                |rows: &[Self]| {
                    Ok(Arc::new(StringArray::from_iter(
                        rows.iter().map(|row| row.category.as_deref()),
                    )))
                },
            ),
            Column::required(RefsColumn::Tree.name(), tree_type(), |rows: &[Self]| {
                tree_values(rows.iter().map(|row| row.tree))
            }),
            Column::required(
                RefsColumn::Subflow.name(),
                DataType::UInt32,
                |rows: &[Self]| {
                    Ok(Arc::new(UInt32Array::from_iter_values(
                        rows.iter().map(|row| u32::from(row.subflow)),
                    )))
                },
            ),
            Column::required(RefsColumn::Ts.name(), ts_type(), |rows: &[Self]| {
                Ok(ts_values(rows.iter().map(|row| row.time)))
            }),
        ]
    }

    fn approximate_bytes(&self) -> usize {
        self.ref_type.len()
            + self.domain_id.len()
            + self.service.len()
            + self.category.as_ref().map_or(0, String::len)
    }
}
