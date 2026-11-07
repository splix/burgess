//! Reads files written by the real writer, and checks both the rows a read
//! returns and which bytes of the file it asked the store for.

mod recording_store;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt32Array,
};
use arrow_schema::{Field, Schema};
use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::{
    Day, FileStats, ObjectName, SCHEMA_VERSION, SCHEMA_VERSION_KEY, StateEntry, StateRow,
    StatesColumn,
};
use burgess_core::wire::{PayloadType, UnixMillis};
use burgess_writer::batch::SortedBatch;
use burgess_writer::parquet_files::{FileWriter, ParquetConfig};
use bytes::Bytes;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::{KeyValue, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder};
use recording_store::RecordingStore;

use super::*;

const FIRST_MS: u64 = 1_793_000_000_000;

/// Leaf index of `tree`, the first column of the file.
const TREE_COLUMN: usize = 0;

fn tree(byte: u8) -> TreeId {
    TreeId::from([byte; 12])
}

/// A tree of the same moment as the others in its file, as minted ids
/// share their leading time bytes and differ in the random ones after.
fn same_moment(random: u8) -> TreeId {
    TreeId::from([0x01, 0x9a, 0x3c, 0x40, 0x11, 0x22, random, 0, 0, 0, 0, 0])
}

/// `count` rows of one tree over three subflows, every tenth a split.
fn rows_of(tree: TreeId, count: usize) -> Vec<StateRow> {
    (0..count)
        .map(|i| StateRow {
            tree,
            subflow: Subflow::from(i as u32 % 3),
            time: UnixMillis::from(FIRST_MS + i as u64),
            service: "checkout".into(),
            category: format!("step-{}", i % 7),
            entry: if i % 10 == 9 {
                StateEntry::Split {
                    child: Subflow::from(100 + i as u32),
                }
            } else {
                StateEntry::State {
                    meta: BTreeMap::from([("n".into(), i.to_string())]),
                    payload_type: PayloadType::Json,
                    payload: format!(r#"{{"i":{i}}}"#).into_bytes(),
                }
            },
        })
        .collect()
}

/// One states file in a store, and the rows it was written from.
struct Fixture {
    store: Arc<RecordingStore>,
    name: ObjectName,
    bytes: Bytes,
    /// In file order.
    rows: Vec<StateRow>,
}

impl Fixture {
    /// A file of the given trees and row counts, with pages as small as the
    /// writer makes them, so that every tree spans several pages.
    async fn new(trees: &[(TreeId, usize)], max_row_group_rows: usize) -> Self {
        let mut rows: Vec<StateRow> = trees
            .iter()
            .flat_map(|&(tree, count)| rows_of(tree, count))
            .collect();
        rows.sort_by_key(StateRow::sort_key);
        let writer = FileWriter::new(ParquetConfig {
            max_row_group_rows: NonZeroUsize::new(max_row_group_rows).unwrap(),
            data_page_bytes: NonZeroUsize::new(1).unwrap(),
            ..ParquetConfig::default()
        })
        .unwrap();
        let batch = SortedBatch {
            states: rows.clone(),
            ..SortedBatch::default()
        };
        let bytes = Bytes::from(writer.write_batch(&batch).unwrap().states.bytes);
        Self::stored(bytes, rows).await
    }

    async fn stored(bytes: Bytes, rows: Vec<StateRow>) -> Self {
        let store = Arc::new(RecordingStore::default());
        let name = ObjectName::StatesFile {
            day: "20261107".parse::<Day>().unwrap(),
            nnnn: UnixMillis::from(FIRST_MS),
        };
        store
            .inner
            .put(&Path::from(name.to_string()), bytes.clone().into())
            .await
            .unwrap();
        Fixture {
            store,
            name,
            bytes,
            rows,
        }
    }

    /// The same rows stored again after `change` rewrote the decoded file,
    /// encoded with `properties`.
    async fn rewritten(
        &self,
        properties: WriterProperties,
        change: impl FnOnce(RecordBatch) -> RecordBatch,
    ) -> Self {
        Self::stored(rewrite(&self.bytes, properties, change), self.rows.clone()).await
    }

    fn stats(&self) -> FileStats {
        FileStats {
            rows: self.rows.len() as u64,
            bytes: self.bytes.len() as u64,
        }
    }

    fn metadata(&self) -> ParquetMetaData {
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&self.bytes)
            .unwrap()
    }

    /// A config whose first request fetches exactly the bytes after the last
    /// data page, so any data page fetched shows up as its own range.
    fn tail_only_config(&self) -> StatesFileConfig {
        let tail = self.bytes.len() - data_end(&self.metadata()) as usize;
        StatesFileConfig {
            tail_bytes: NonZeroUsize::new(tail).unwrap(),
        }
    }

    async fn read_with(
        &self,
        config: StatesFileConfig,
        tree: TreeId,
    ) -> Result<Vec<StateRow>, TreeReadError> {
        StatesFileReader::new(self.store.clone(), config)
            .tree(&self.name, self.stats(), tree)
            .await
    }

    async fn read(&self, tree: TreeId) -> Vec<StateRow> {
        self.read_with(self.tail_only_config(), tree).await.unwrap()
    }

    async fn error(&self, tree: TreeId) -> TreeReadError {
        let error = self
            .read_with(StatesFileConfig::default(), tree)
            .await
            .unwrap_err();
        assert_eq!(error.file(), &self.name);
        error
    }

    fn expected(&self, tree: TreeId) -> Vec<StateRow> {
        self.rows
            .iter()
            .filter(|row| row.tree == tree)
            .cloned()
            .collect()
    }

    /// The file rows of `tree`, which the rows are sorted by.
    fn rows_of_tree(&self, tree: TreeId) -> Range<i64> {
        let start = self.rows.iter().position(|row| row.tree == tree).unwrap();
        let count = self.rows.iter().filter(|row| row.tree == tree).count();
        start as i64..(start + count) as i64
    }
}

/// Writer settings declaring `version` as the layout version, if any.
fn layout(version: Option<&str>) -> WriterPropertiesBuilder {
    WriterProperties::builder().set_key_value_metadata(version.map(|version| {
        vec![KeyValue::new(
            SCHEMA_VERSION_KEY.into(),
            version.to_string(),
        )]
    }))
}

/// Decodes `bytes` into one batch, applies `change`, and encodes the result
/// with `properties`.
fn rewrite(
    bytes: &Bytes,
    properties: WriterProperties,
    change: impl FnOnce(RecordBatch) -> RecordBatch,
) -> Bytes {
    let mut batches = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .with_batch_size(usize::MAX)
        .build()
        .unwrap();
    let batch = change(batches.next().unwrap().unwrap());
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    Bytes::from(writer.into_inner().unwrap())
}

/// `batch` with the values of `column` replaced, keeping the column's name
/// and nullability but taking the type of `values`.
fn with_column(batch: RecordBatch, column: StatesColumn, values: ArrayRef) -> RecordBatch {
    let schema = batch.schema();
    let index = schema.index_of(column.name()).unwrap();
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields[index] = Field::new(
        column.name(),
        values.data_type().clone(),
        fields[index].is_nullable(),
    );
    let mut columns = batch.columns().to_vec();
    columns[index] = values;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// A rewrite that changes only `column`, to `values` built for each row.
async fn with_values(
    written: &Fixture,
    column: StatesColumn,
    values: impl FnOnce(usize) -> ArrayRef,
) -> Fixture {
    written
        .rewritten(layout(Some(SCHEMA_VERSION)).build(), |batch| {
            let values = values(batch.num_rows());
            with_column(batch, column, values)
        })
        .await
}

/// One data page of one column.
struct Page {
    /// Leaf index of the column.
    column: usize,
    /// Rows it holds, counted from the start of the file.
    rows: Range<i64>,
    /// Where it is in the file.
    bytes: Range<u64>,
}

/// Every data page of every column.
fn pages(metadata: &ParquetMetaData) -> Vec<Page> {
    let mut pages = Vec::new();
    let mut first_row = 0;
    for (index, row_group) in metadata.row_groups().iter().enumerate() {
        let page_index = metadata.page_index_for_row_group(index);
        for column in 0..row_group.num_columns() {
            let locations = page_index.page_locations(column).unwrap();
            for (page, location) in locations.iter().enumerate() {
                let end = locations
                    .get(page + 1)
                    .map_or(row_group.num_rows(), |next| next.first_row_index);
                let offset = location.offset as u64;
                pages.push(Page {
                    column,
                    rows: first_row + location.first_row_index..first_row + end,
                    bytes: offset..offset + location.compressed_page_size as u64,
                });
            }
        }
        first_row += row_group.num_rows();
    }
    pages
}

/// Where the last column chunk ends: everything after is index and footer.
fn data_end(metadata: &ParquetMetaData) -> u64 {
    metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .map(|column| {
            let (start, length) = column.byte_range();
            start + length
        })
        .max()
        .unwrap()
}

fn overlaps<T: Ord>(a: &Range<T>, b: &Range<T>) -> bool {
    a.start < b.end && b.start < a.end
}

fn fetched(store: &RecordingStore, bytes: &Range<u64>) -> bool {
    store.ranges().iter().any(|range| overlaps(range, bytes))
}

#[tokio::test]
async fn returns_exactly_the_rows_of_the_tree_in_file_order() {
    let fixture = Fixture::new(&[(tree(1), 3000), (tree(2), 40), (tree(3), 3000)], 1 << 20).await;
    for byte in [1, 2, 3] {
        assert_eq!(
            fixture.read(tree(byte)).await,
            fixture.expected(tree(byte)),
            "tree {byte}"
        );
    }
}

#[tokio::test]
async fn returns_states_and_splits_as_written() {
    let fixture = Fixture::new(&[(tree(2), 20)], 1 << 20).await;
    let rows = fixture.read(tree(2)).await;
    assert_eq!(rows, fixture.expected(tree(2)));
    assert!(
        rows.iter()
            .any(|row| matches!(row.entry, StateEntry::Split { .. }))
    );
}

#[tokio::test]
async fn reads_a_tree_spread_over_row_groups() {
    let fixture = Fixture::new(&[(tree(1), 500), (tree(2), 2500), (tree(3), 500)], 1000).await;
    assert!(fixture.metadata().num_row_groups() > 3);
    assert_eq!(fixture.read(tree(2)).await, fixture.expected(tree(2)));
}

#[tokio::test]
async fn fetches_only_the_pages_that_can_hold_the_tree() {
    let fixture = Fixture::new(&[(tree(1), 3000), (tree(2), 40), (tree(3), 3000)], 1 << 20).await;
    let pages = pages(&fixture.metadata());
    let tree_rows = fixture.rows_of_tree(tree(2));
    // The pages of `tree` holding the tree decide which rows are read; every
    // other column is fetched only where its pages overlap those rows.
    let tree_pages: Vec<&Page> = pages
        .iter()
        .filter(|page| page.column == TREE_COLUMN && overlaps(&page.rows, &tree_rows))
        .collect();
    let wanted = tree_pages.first().unwrap().rows.start..tree_pages.last().unwrap().rows.end;
    assert!(
        pages
            .iter()
            .filter(|page| !overlaps(&page.rows, &wanted))
            .count()
            > 10,
        "the file must have pages to skip"
    );

    fixture.read(tree(2)).await;

    for page in &pages {
        assert_eq!(
            fetched(&fixture.store, &page.bytes),
            overlaps(&page.rows, &wanted),
            "page of rows {:?} at {:?}",
            page.rows,
            page.bytes
        );
    }
    // The tail, then the pages of the one row group holding the tree.
    assert_eq!(fixture.store.requests().len(), 2);
}

#[tokio::test]
async fn tree_outside_the_file_bounds_reads_no_data_page() {
    let fixture = Fixture::new(&[(tree(2), 3000), (tree(4), 3000)], 1 << 20).await;
    for byte in [1, 5] {
        assert_eq!(fixture.read(tree(byte)).await, [], "tree {byte}");
    }
    assert_eq!(fixture.store.requests().len(), 2, "one request per read");
    let data_end = data_end(&fixture.metadata());
    for range in fixture.store.ranges() {
        assert!(
            range.start >= data_end,
            "{range:?} reads data before {data_end}"
        );
    }
}

#[tokio::test]
async fn tree_between_row_groups_reads_no_data_page() {
    let fixture = Fixture::new(&[(tree(2), 1000), (tree(4), 1000)], 1000).await;
    assert_eq!(fixture.metadata().num_row_groups(), 2);
    assert_eq!(fixture.read(tree(3)).await, []);
    let data_end = data_end(&fixture.metadata());
    assert!(
        fixture
            .store
            .ranges()
            .iter()
            .all(|range| range.start >= data_end)
    );
}

#[tokio::test]
async fn tree_between_pages_reads_no_data_page() {
    let fixture = Fixture::new(&[(tree(2), 3072), (tree(4), 3000)], 1 << 20).await;
    let tree_page_starts: Vec<i64> = pages(&fixture.metadata())
        .iter()
        .filter(|page| page.column == TREE_COLUMN)
        .map(|page| page.rows.start)
        .collect();
    assert!(
        tree_page_starts.contains(&fixture.rows_of_tree(tree(4)).start),
        "a page must start where tree 4 starts, so tree 3 falls between pages"
    );

    assert_eq!(fixture.read(tree(3)).await, []);

    let data_end = data_end(&fixture.metadata());
    assert!(
        fixture
            .store
            .ranges()
            .iter()
            .all(|range| range.start >= data_end)
    );
}

#[tokio::test]
async fn without_page_bounds_row_group_bounds_still_rule_out_a_tree() {
    let written = Fixture::new(&[(tree(2), 1000), (tree(4), 1000)], 1 << 20).await;
    let properties = layout(Some(SCHEMA_VERSION))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(1000))
        .build();
    let fixture = written.rewritten(properties, |batch| batch).await;
    let metadata = fixture.metadata();
    assert_eq!(metadata.num_row_groups(), 2);
    assert!(
        metadata
            .page_index_for_row_group(0)
            .column_index(TREE_COLUMN)
            .is_none()
    );

    assert_eq!(fixture.read(tree(3)).await, []);
    let data_end = data_end(&metadata);
    assert!(
        fixture
            .store
            .ranges()
            .iter()
            .all(|range| range.start >= data_end)
    );

    assert_eq!(fixture.read(tree(4)).await, fixture.expected(tree(4)));
    let (start, length) = metadata.row_group(0).column(TREE_COLUMN).byte_range();
    assert!(!fetched(&fixture.store, &(start..start + length)));
}

#[tokio::test]
async fn tree_inside_a_page_but_absent_gives_no_rows() {
    let fixture = Fixture::new(&[(tree(2), 30), (tree(4), 30)], 1 << 20).await;
    assert_eq!(fixture.read(tree(3)).await, []);
}

#[tokio::test]
async fn a_short_tail_still_reads_the_tree() {
    let fixture = Fixture::new(&[(tree(1), 300), (tree(2), 40)], 1 << 20).await;
    let config = StatesFileConfig {
        tail_bytes: NonZeroUsize::new(8).unwrap(),
    };
    let rows = fixture.read_with(config, tree(2)).await.unwrap();
    assert_eq!(rows, fixture.expected(tree(2)));
}

#[tokio::test]
async fn rejects_a_file_of_another_layout_version() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let newer = written
        .rewritten(layout(Some("2")).build(), |batch| batch)
        .await;
    assert!(matches!(
        newer.error(tree(2)).await.problem(),
        StatesFileProblem::UnknownLayout { found: Some(version) } if version == "2"
    ));
    let unversioned = written.rewritten(layout(None).build(), |batch| batch).await;
    assert!(matches!(
        unversioned.error(tree(2)).await.problem(),
        StatesFileProblem::UnknownLayout { found: None }
    ));
}

#[tokio::test]
async fn rejects_a_file_with_other_rows_than_the_manifest_lists() {
    let mut fixture = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    fixture.rows.pop();
    assert!(matches!(
        fixture.error(tree(2)).await.problem(),
        StatesFileProblem::RowCount {
            listed: 9,
            found: 10
        }
    ));
}

#[tokio::test]
async fn rejects_an_unknown_kind_and_says_which_row() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = with_values(&written, StatesColumn::Kind, |rows| {
        Arc::new(StringArray::from(vec!["event"; rows]))
    })
    .await;
    let first = &written.expected(tree(2))[0];
    match fixture.error(tree(2)).await.problem() {
        StatesFileProblem::Row {
            subflow,
            ts_millis,
            problem: InvalidRow::UnknownKind(kind),
        } => {
            assert_eq!(kind, "event");
            assert_eq!(*subflow, first.subflow);
            assert_eq!(*ts_millis, u64::from(first.time) as i64);
        }
        other => panic!("unexpected problem: {other:?}"),
    }
}

/// The row problem a read of tree 2 fails with, after `values` replaced
/// `column` in a file of ten rows.
async fn row_problem(column: StatesColumn, values: impl FnOnce(usize) -> ArrayRef) -> InvalidRow {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = with_values(&written, column, values).await;
    match fixture.error(tree(2)).await.problem() {
        StatesFileProblem::Row { problem, .. } => problem.clone(),
        other => panic!("unexpected problem: {other:?}"),
    }
}

#[tokio::test]
async fn rejects_a_split_without_child() {
    let problem = row_problem(StatesColumn::ChildSubflow, |rows| {
        Arc::new(UInt32Array::from(vec![None; rows]))
    })
    .await;
    assert_eq!(problem, InvalidRow::SplitWithoutChild);
}

#[tokio::test]
async fn rejects_a_state_without_payload() {
    let problem = row_problem(StatesColumn::Payload, |rows| {
        Arc::new(BinaryArray::from(vec![None::<&[u8]>; rows]))
    })
    .await;
    assert_eq!(problem, InvalidRow::StateWithoutPayload);
}

#[tokio::test]
async fn rejects_an_unknown_payload_type() {
    let problem = row_problem(StatesColumn::PayloadType, |rows| {
        Arc::new(StringArray::from(vec![Some("xml"); rows]))
    })
    .await;
    assert_eq!(problem, InvalidRow::UnknownPayloadType("xml".into()));
}

#[tokio::test]
async fn rejects_a_time_before_the_epoch() {
    let problem = row_problem(StatesColumn::Ts, |rows| {
        Arc::new(TimestampMillisecondArray::from(vec![-1; rows]).with_timezone("UTC"))
    })
    .await;
    assert_eq!(problem, InvalidRow::TimeBeforeEpoch);
}

#[tokio::test]
async fn rejects_a_tree_column_of_another_width() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = with_values(&written, StatesColumn::Tree, |rows| {
        let wide = (0..rows).map(|_| [2; 16]);
        Arc::new(FixedSizeBinaryArray::try_from_iter(wide).unwrap())
    })
    .await;
    assert!(matches!(
        fixture.error(tree(2)).await.problem(),
        StatesFileProblem::Column { column: "tree" }
    ));
}

#[tokio::test]
async fn a_missing_file_is_a_store_problem() {
    let fixture = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let gone = ObjectName::StatesFile {
        day: "20261107".parse().unwrap(),
        nnnn: UnixMillis::from(FIRST_MS + 1),
    };
    let error = StatesFileReader::new(fixture.store.clone(), StatesFileConfig::default())
        .tree(&gone, fixture.stats(), tree(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error.problem(),
        StatesFileProblem::Store(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn ids_compare_by_unsigned_bytes_after_a_shared_prefix() {
    let present = [same_moment(0x00), same_moment(0x80), same_moment(0xff)];
    let fixture = Fixture::new(
        &[(present[0], 1500), (present[1], 40), (present[2], 1500)],
        1 << 20,
    )
    .await;
    for tree in present {
        assert_eq!(fixture.read(tree).await, fixture.expected(tree), "{tree}");
    }
    for absent in [same_moment(0x7f), same_moment(0x81)] {
        assert_eq!(fixture.read(absent).await, [], "{absent}");
    }
}

#[tokio::test]
async fn reads_a_file_without_an_offset_index() {
    let written = Fixture::new(&[(tree(2), 1000), (tree(4), 1000)], 1 << 20).await;
    let properties = layout(Some(SCHEMA_VERSION))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_offset_index_disabled(true)
        .set_max_row_group_row_count(Some(1000))
        .build();
    let fixture = written.rewritten(properties, |batch| batch).await;
    let metadata = fixture.metadata();
    assert!(
        metadata
            .page_index_for_row_group(0)
            .offset_index(TREE_COLUMN)
            .is_none()
    );

    assert_eq!(fixture.read(tree(4)).await, fixture.expected(tree(4)));
    assert_eq!(fixture.read(tree(3)).await, []);
}

#[tokio::test]
async fn rejects_a_file_without_the_tree_column() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = written
        .rewritten(layout(Some(SCHEMA_VERSION)).build(), |batch| {
            let mut batch = batch;
            batch.remove_column(batch.schema().index_of(StatesColumn::Tree.name()).unwrap());
            batch
        })
        .await;
    assert!(matches!(
        fixture.error(tree(2)).await.problem(),
        StatesFileProblem::Column { column: "tree" }
    ));
}
