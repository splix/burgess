//! Which states of a file a scan keeps.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{BooleanArray, MapArray, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{ArrowError, Schema};
use burgess_core::storage::{EntryKind, StatesColumn};
use jiff::Timestamp;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderMetadata, RowFilter};

use super::decode::{Meta, column};
use super::error::StatesFileProblem;
use crate::parquet_file::FileProblem;

/// The states to keep: of one service and category, in a time window, with
/// some labels.
///
/// The `(tree, subflow, ts)` order of the file does not help to find them,
/// thus a scan reads these columns of each row. The payload is read only
/// for the rows that pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatesFilter {
    /// The service that captured the state, as stored.
    pub service: String,
    /// The category of the state, as stored.
    pub category: String,
    /// Labels that the state must have, each with this value.
    pub meta: BTreeMap<String, String>,
    /// The earliest time, by the producer clock.
    pub since: Timestamp,
    /// The latest time, by the producer clock.
    pub until: Timestamp,
}

impl StatesFilter {
    /// The filter as the decoder applies it to the rows of a file.
    ///
    /// The columns are checked here, before any row is read, thus a file of
    /// another layout gives the same problem as when the rows are decoded.
    pub(super) fn row_filter(
        self: &Arc<Self>,
        metadata: &ArrowReaderMetadata,
    ) -> Result<RowFilter, StatesFileProblem> {
        let columns = self.columns();
        let fields = columns
            .iter()
            .map(|column| {
                metadata
                    .schema()
                    .field_with_name(column.name())
                    .cloned()
                    .map_err(|_| FileProblem::Column {
                        column: column.name(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(fields)));
        FilterColumns::new(&empty, self.has_meta())?;
        let mask = ProjectionMask::columns(
            metadata.parquet_schema(),
            columns.iter().map(|column| column.name()),
        );
        let filter = Arc::clone(self);
        let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            filter
                .passes(&batch)
                .map_err(|problem| ArrowError::ExternalError(Box::new(problem)))
        });
        Ok(RowFilter::new(vec![Box::new(predicate)]))
    }

    /// The columns that the filter reads. Without labels to match, the
    /// labels of a row are not read.
    fn columns(&self) -> Vec<StatesColumn> {
        let mut columns = vec![
            StatesColumn::Kind,
            StatesColumn::Service,
            StatesColumn::Category,
            StatesColumn::Ts,
        ];
        if self.has_meta() {
            columns.push(StatesColumn::Meta);
        }
        columns
    }

    fn has_meta(&self) -> bool {
        !self.meta.is_empty()
    }

    /// For each row of `batch`, whether it is a row that the filter keeps.
    ///
    /// Only splits are refused by their kind. A row of an unknown kind is
    /// kept, thus the decoder reports it, as a tree read does.
    fn passes(&self, batch: &RecordBatch) -> Result<BooleanArray, StatesFileProblem> {
        let rows = FilterColumns::new(batch, self.has_meta())?;
        let (since, until) = (self.since.as_millisecond(), self.until.as_millisecond());
        Ok((0..batch.num_rows())
            .map(|row| {
                Some(
                    rows.kind.value(row) != EntryKind::Split.name()
                        && rows.service.value(row) == self.service
                        && rows.category.value(row) == self.category
                        && (since..=until).contains(&rows.ts.value(row))
                        && rows
                            .meta
                            .as_ref()
                            .is_none_or(|meta| self.has_labels(meta, row)),
                )
            })
            .collect())
    }

    /// Whether the labels of `row` include each label of the filter.
    fn has_labels(&self, meta: &Meta<'_>, row: usize) -> bool {
        let labels: Vec<(&str, &str)> = meta.entries(row).collect();
        self.meta
            .iter()
            .all(|(key, value)| labels.contains(&(key.as_str(), value.as_str())))
    }
}

/// The columns of a batch that the filter reads, typed.
struct FilterColumns<'a> {
    kind: &'a StringArray,
    service: &'a StringArray,
    category: &'a StringArray,
    ts: &'a TimestampMillisecondArray,
    /// Only when the filter has labels to match.
    meta: Option<Meta<'a>>,
}

impl<'a> FilterColumns<'a> {
    fn new(batch: &'a RecordBatch, with_meta: bool) -> Result<Self, StatesFileProblem> {
        let meta = if with_meta {
            Some(Meta::try_from(column::<MapArray>(
                batch,
                StatesColumn::Meta,
            )?)?)
        } else {
            None
        };
        Ok(FilterColumns {
            kind: column(batch, StatesColumn::Kind)?,
            service: column(batch, StatesColumn::Service)?,
            category: column(batch, StatesColumn::Category)?,
            ts: column(batch, StatesColumn::Ts)?,
            meta,
        })
    }
}
