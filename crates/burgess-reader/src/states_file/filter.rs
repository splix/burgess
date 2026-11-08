//! Which states of a file a scan keeps.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicUsize};

use arrow_array::{
    Array, BinaryArray, BooleanArray, MapArray, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::ArrowError;
use burgess_core::storage::{EntryKind, StatesColumn};
use burgess_core::wire::PayloadType;
use jiff::Timestamp;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, RowFilter,
};
use serde_json::Value;

use super::decode::{Meta, column};
use super::error::StatesFileProblem;
use crate::filter::Filter;

/// The states to keep: of one service and category, in a time window, with
/// some labels, and a payload that passes a filter.
///
/// The `(tree, subflow, ts)` order of the file does not help to find them,
/// thus a scan reads these columns of each row. The payload is read, and
/// its filter applied, only for the rows that pass. The other columns are
/// read only for the rows that pass both.
#[derive(Debug, Clone, PartialEq)]
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
    /// A filter on the payload, if any.
    pub payload: Option<Filter>,
}

impl StatesFilter {
    /// The filter as the decoder applies it to the rows of a file, whose
    /// columns are checked.
    ///
    /// The payload filter is a second step, which the decoder runs only on
    /// the rows that pass the first. Thus the decoder can stop at a limit of
    /// rows that pass both, and a row that fails is never decoded in full.
    /// `untested` counts the payloads that are not JSON, which pass no
    /// payload filter.
    pub(super) fn row_filter(
        self: &Arc<Self>,
        metadata: &ArrowReaderMetadata,
        untested: &Arc<AtomicUsize>,
    ) -> RowFilter {
        let schema = metadata.parquet_schema();
        let columns = self.columns();
        let mask = ProjectionMask::columns(schema, columns.iter().map(|column| column.name()));
        let filter = Arc::clone(self);
        let columns = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            filter.passes(&batch).map_err(external)
        });
        let mut predicates: Vec<Box<dyn ArrowPredicate>> = vec![Box::new(columns)];
        if let Some(payload) = &self.payload {
            let mask = ProjectionMask::columns(
                schema,
                [
                    StatesColumn::PayloadType.name(),
                    StatesColumn::Payload.name(),
                ],
            );
            let payload = payload.clone();
            let untested = Arc::clone(untested);
            predicates.push(Box::new(ArrowPredicateFn::new(
                mask,
                move |batch: RecordBatch| {
                    payloads_pass(&payload, &batch, &untested).map_err(external)
                },
            )));
        }
        RowFilter::new(predicates)
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

/// For each row of `batch`, whether its payload passes `filter`. A payload
/// that is not JSON is counted in `untested`.
fn payloads_pass(
    filter: &Filter,
    batch: &RecordBatch,
    untested: &AtomicUsize,
) -> Result<BooleanArray, StatesFileProblem> {
    let payload_type: &StringArray = column(batch, StatesColumn::PayloadType)?;
    let payload: &BinaryArray = column(batch, StatesColumn::Payload)?;
    Ok((0..batch.num_rows())
        .map(|row| {
            let json = !payload_type.is_null(row)
                && !payload.is_null(row)
                && payload_type.value(row) == PayloadType::Json.name();
            let value = json
                .then(|| serde_json::from_slice::<Value>(payload.value(row)).ok())
                .flatten();
            let Some(value) = value else {
                untested.fetch_add(1, atomic::Ordering::Relaxed);
                return Some(false);
            };
            Some(filter.matches(&value))
        })
        .collect())
}

/// A problem as the decoder takes it from a predicate.
fn external(problem: StatesFileProblem) -> ArrowError {
    ArrowError::ExternalError(Box::new(problem))
}
