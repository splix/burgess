//! Turning decoded Arrow columns back into the rows the writer wrote.

use std::collections::BTreeMap;

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, MapArray, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt32Array,
};
use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::{EntryKind, StateEntry, StateRow, StatesColumn};
use burgess_core::wire::{PayloadType, UnixMillis};

use super::error::StatesFileProblem;

/// A row that breaks the rules the writer follows, so it cannot be returned
/// as a state or a split.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidRow {
    /// The `kind` column holds neither `state` nor `split`.
    #[error("unknown kind {0:?}")]
    UnknownKind(String),
    /// A split without the subflow of its child.
    #[error("a split has no child subflow")]
    SplitWithoutChild,
    /// A state without a payload or its type.
    #[error("a state has no payload")]
    StateWithoutPayload,
    /// The payload type is not one this reader knows.
    #[error("unknown payload type {0:?}")]
    UnknownPayloadType(String),
    /// The writer stores only times from the Unix epoch on.
    #[error("the time is before the Unix epoch")]
    TimeBeforeEpoch,
}

/// The columns of one decoded batch of a states file, typed.
pub(super) struct StatesBatch<'a> {
    tree: &'a FixedSizeBinaryArray,
    subflow: &'a UInt32Array,
    ts: &'a TimestampMillisecondArray,
    kind: &'a StringArray,
    service: &'a StringArray,
    category: &'a StringArray,
    child_subflow: &'a UInt32Array,
    meta: Meta<'a>,
    payload_type: &'a StringArray,
    payload: &'a BinaryArray,
}

/// The `meta` map column with its keys and values typed.
struct Meta<'a> {
    map: &'a MapArray,
    keys: &'a StringArray,
    values: &'a StringArray,
}

impl<'a> TryFrom<&'a RecordBatch> for StatesBatch<'a> {
    type Error = StatesFileProblem;

    fn try_from(batch: &'a RecordBatch) -> Result<Self, Self::Error> {
        Ok(StatesBatch {
            tree: column(batch, StatesColumn::Tree)?,
            subflow: column(batch, StatesColumn::Subflow)?,
            ts: column(batch, StatesColumn::Ts)?,
            kind: column(batch, StatesColumn::Kind)?,
            service: column(batch, StatesColumn::Service)?,
            category: column(batch, StatesColumn::Category)?,
            child_subflow: column(batch, StatesColumn::ChildSubflow)?,
            meta: Meta::try_from(column::<MapArray>(batch, StatesColumn::Meta)?)?,
            payload_type: column(batch, StatesColumn::PayloadType)?,
            payload: column(batch, StatesColumn::Payload)?,
        })
    }
}

impl<'a> TryFrom<&'a MapArray> for Meta<'a> {
    type Error = StatesFileProblem;

    fn try_from(map: &'a MapArray) -> Result<Self, Self::Error> {
        let mismatch = StatesFileProblem::Column {
            column: StatesColumn::Meta.name(),
        };
        let keys = map.keys().as_any().downcast_ref::<StringArray>();
        let values = map.values().as_any().downcast_ref::<StringArray>();
        match (keys, values) {
            (Some(keys), Some(values)) => Ok(Meta { map, keys, values }),
            _ => Err(mismatch),
        }
    }
}

impl Meta<'_> {
    fn labels(&self, row: usize) -> BTreeMap<String, String> {
        let offsets = self.map.value_offsets();
        // Arrow checks on decoding that offsets are ascending from zero.
        (offsets[row] as usize..offsets[row + 1] as usize)
            .map(|entry| {
                (
                    self.keys.value(entry).to_string(),
                    self.values.value(entry).to_string(),
                )
            })
            .collect()
    }
}

impl StatesBatch<'_> {
    /// The rows of `tree`, in the order the batch holds them.
    pub(super) fn rows_of(&self, tree: TreeId) -> Result<Vec<StateRow>, StatesFileProblem> {
        (0..self.tree.len())
            .filter(|&row| self.tree.value(row) == tree.as_bytes().as_slice())
            .map(|row| {
                self.row(tree, row)
                    .map_err(|problem| StatesFileProblem::Row {
                        subflow: Subflow::from(self.subflow.value(row)),
                        ts_millis: self.ts.value(row),
                        problem,
                    })
            })
            .collect()
    }

    fn row(&self, tree: TreeId, row: usize) -> Result<StateRow, InvalidRow> {
        let millis = self.ts.value(row);
        let time = u64::try_from(millis).map_err(|_| InvalidRow::TimeBeforeEpoch)?;
        Ok(StateRow {
            tree,
            subflow: Subflow::from(self.subflow.value(row)),
            time: UnixMillis::from(time),
            service: self.service.value(row).to_string(),
            category: self.category.value(row).to_string(),
            entry: self.entry(row)?,
        })
    }

    fn entry(&self, row: usize) -> Result<StateEntry, InvalidRow> {
        let kind = self.kind.value(row);
        match EntryKind::from_name(kind) {
            Some(EntryKind::State) => self.state(row),
            Some(EntryKind::Split) => self.split(row),
            None => Err(InvalidRow::UnknownKind(kind.to_string())),
        }
    }

    fn state(&self, row: usize) -> Result<StateEntry, InvalidRow> {
        if self.payload_type.is_null(row) || self.payload.is_null(row) {
            return Err(InvalidRow::StateWithoutPayload);
        }
        let name = self.payload_type.value(row);
        let payload_type = PayloadType::from_name(name)
            .ok_or_else(|| InvalidRow::UnknownPayloadType(name.to_string()))?;
        Ok(StateEntry::State {
            meta: self.meta.labels(row),
            payload_type,
            payload: self.payload.value(row).to_vec(),
        })
    }

    fn split(&self, row: usize) -> Result<StateEntry, InvalidRow> {
        if self.child_subflow.is_null(row) {
            return Err(InvalidRow::SplitWithoutChild);
        }
        Ok(StateEntry::Split {
            child: Subflow::from(self.child_subflow.value(row)),
        })
    }
}

/// The column `name` of `batch` as an `A`.
fn column<A: Array + 'static>(
    batch: &RecordBatch,
    name: StatesColumn,
) -> Result<&A, StatesFileProblem> {
    batch
        .column_by_name(name.name())
        .and_then(|array| array.as_any().downcast_ref::<A>())
        .ok_or(StatesFileProblem::Column {
            column: name.name(),
        })
}
