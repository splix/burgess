//! Turning decoded Arrow columns back into the references the writer wrote.

use arrow_array::{
    Array, FixedSizeBinaryArray, RecordBatch, StringArray, TimestampMillisecondArray, UInt32Array,
};
use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::{RefRow, RefsColumn};
use burgess_core::wire::UnixMillis;

use super::error::RefsFileProblem;
use super::key::RefKey;
use crate::parquet_file::{FileProblem, Numbered, RowNumbers, row_numbers};

/// The columns of one decoded batch of a refs file, typed.
pub(super) struct RefsBatch<'a> {
    ref_type: &'a StringArray,
    domain_id: &'a StringArray,
    service: &'a StringArray,
    category: &'a StringArray,
    tree: &'a FixedSizeBinaryArray,
    subflow: &'a UInt32Array,
    ts: &'a TimestampMillisecondArray,
    numbers: RowNumbers<'a>,
}

impl<'a> TryFrom<&'a RecordBatch> for RefsBatch<'a> {
    type Error = RefsFileProblem;

    fn try_from(batch: &'a RecordBatch) -> Result<Self, Self::Error> {
        let tree: &FixedSizeBinaryArray = column(batch, RefsColumn::Tree)?;
        if usize::try_from(tree.value_length()) != Ok(TreeId::LEN) {
            return Err(column_problem(RefsColumn::Tree));
        }
        Ok(RefsBatch {
            ref_type: column(batch, RefsColumn::RefType)?,
            domain_id: column(batch, RefsColumn::DomainId)?,
            service: column(batch, RefsColumn::Service)?,
            category: column(batch, RefsColumn::Category)?,
            tree,
            subflow: column(batch, RefsColumn::Subflow)?,
            ts: column(batch, RefsColumn::Ts)?,
            numbers: row_numbers(batch)?,
        })
    }
}

impl RefsBatch<'_> {
    /// The references to `key`, in the order the batch holds them.
    pub(super) fn rows_of(&self, key: &RefKey) -> Result<Vec<Numbered<RefRow>>, RefsFileProblem> {
        (0..self.ref_type.len())
            .filter(|&row| {
                self.ref_type.value(row) == key.ref_type()
                    && self.domain_id.value(row) == key.domain_id()
            })
            .map(|row| {
                Ok(Numbered {
                    number: self.numbers.of(row),
                    row: self.row(row)?,
                })
            })
            .collect()
    }

    fn row(&self, row: usize) -> Result<RefRow, RefsFileProblem> {
        let ts_millis = self.ts.value(row);
        let time =
            u64::try_from(ts_millis).map_err(|_| RefsFileProblem::TimeBeforeEpoch { ts_millis })?;
        let tree: [u8; TreeId::LEN] = self
            .tree
            .value(row)
            .try_into()
            .expect("the width of the column is checked");
        Ok(RefRow {
            ref_type: self.ref_type.value(row).to_string(),
            domain_id: self.domain_id.value(row).to_string(),
            service: self.service.value(row).to_string(),
            category: (!self.category.is_null(row)).then(|| self.category.value(row).to_string()),
            tree: TreeId::from(tree),
            subflow: Subflow::from(self.subflow.value(row)),
            time: UnixMillis::from(time),
        })
    }
}

/// The column `name` of `batch` as an `A`.
fn column<A: Array + 'static>(
    batch: &RecordBatch,
    name: RefsColumn,
) -> Result<&A, RefsFileProblem> {
    batch
        .column_by_name(name.name())
        .and_then(|array| array.as_any().downcast_ref::<A>())
        .ok_or_else(|| column_problem(name))
}

fn column_problem(name: RefsColumn) -> RefsFileProblem {
    FileProblem::Column {
        column: name.name(),
    }
    .into()
}
