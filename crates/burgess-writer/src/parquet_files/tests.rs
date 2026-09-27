//! Reads written files back with the `parquet` crate, the way the query
//! service will, and checks both the rows and the layout lookups rely on.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use arrow_array::{Array, RecordBatch};
use arrow_select::concat::concat_batches;
use burgess_core::flow::{FlowId, Subflow};
use burgess_core::wire::{PayloadType, UnixMillis};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, SortingColumn};
use parquet::file::statistics::Statistics;

use super::columns::Column;
use super::layout::FileLayout;
use super::table::Table;
use super::*;
use crate::batch::{RefRow, SortedBatch, StateEntry, StateRow};

const ROOT: &str = "0192f3a1b2c3d4e5f6a7b8c900000000";
const CHILD: &str = "0192f3a1b2c3d4e5f6a7b8c900000007";

fn flow(id: &str) -> FlowId {
    id.parse().unwrap()
}

fn state_rows() -> Vec<StateRow> {
    vec![
        StateRow {
            tree: flow(ROOT).tree(),
            subflow: flow(ROOT).subflow(),
            time: UnixMillis::from(1_790_000_000_000),
            service: "checkout".into(),
            category: "payment".into(),
            entry: StateEntry::State {
                meta: BTreeMap::from([
                    ("currency".into(), "EUR".into()),
                    ("region".into(), "eu".into()),
                ]),
                payload_type: PayloadType::Json,
                payload: br#"{"amount":42}"#.to_vec(),
            },
        },
        StateRow {
            tree: flow(ROOT).tree(),
            subflow: flow(ROOT).subflow(),
            time: UnixMillis::from(1_790_000_000_500),
            service: "checkout".into(),
            category: "fan-out".into(),
            entry: StateEntry::Split {
                child: flow(CHILD).subflow(),
            },
        },
        StateRow {
            tree: flow(CHILD).tree(),
            subflow: flow(CHILD).subflow(),
            time: UnixMillis::from(1_790_000_001_000),
            service: "shipping".into(),
            category: "label".into(),
            entry: StateEntry::State {
                meta: BTreeMap::new(),
                payload_type: PayloadType::Json,
                payload: b"{}".to_vec(),
            },
        },
    ]
}

fn ref_rows() -> Vec<RefRow> {
    vec![
        RefRow {
            ref_type: "order_id".into(),
            domain_id: "ORD-1".into(),
            service: "orders".into(),
            category: Some("lookup".into()),
            tree: flow(ROOT).tree(),
            subflow: flow(ROOT).subflow(),
            time: UnixMillis::from(1_790_000_000_000),
        },
        RefRow {
            ref_type: "user_id".into(),
            domain_id: "U-9".into(),
            service: "accounts".into(),
            category: None,
            tree: flow(CHILD).tree(),
            subflow: Subflow::from(7),
            time: UnixMillis::from(1_790_000_002_000),
        },
    ]
}

fn writer() -> FileWriter {
    FileWriter::new(ParquetConfig::default()).unwrap()
}

fn states_file(rows: &[StateRow]) -> ParquetFile {
    writer().states.encode(rows).unwrap()
}

fn refs_file(rows: &[RefRow]) -> ParquetFile {
    writer().refs.encode(rows).unwrap()
}

struct ReadBack {
    metadata: ParquetMetaData,
    batches: Vec<RecordBatch>,
}

fn read(file: &ParquetFile) -> ReadBack {
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        Bytes::from(file.bytes.clone()),
        options,
    )
    .unwrap();
    let metadata = builder.metadata().as_ref().clone();
    let batches = builder.build().unwrap().collect::<Result<_, _>>().unwrap();
    ReadBack { metadata, batches }
}

fn column_index(metadata: &ParquetMetaData, name: &str) -> usize {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.path().string() == name)
        .unwrap_or_else(|| panic!("no column {name}"))
}

#[test]
fn states_round_trip() {
    let rows = state_rows();
    let read = read(&states_file(&rows));
    assert_eq!(read.batches, [writer().states.record_batch(&rows).unwrap()]);
}

#[test]
fn refs_round_trip() {
    let rows = ref_rows();
    let read = read(&refs_file(&rows));
    assert_eq!(read.batches, [writer().refs.record_batch(&rows).unwrap()]);
}

#[test]
fn split_rows_have_no_payload_and_an_empty_meta() {
    let batch = writer().states.record_batch(&state_rows()).unwrap();
    let split = 1;
    for name in ["payload", "payload_type"] {
        assert!(batch.column_by_name(name).unwrap().is_null(split), "{name}");
    }
    assert!(
        batch
            .column_by_name("child_subflow")
            .unwrap()
            .is_valid(split)
    );
    let meta = batch
        .column_by_name("meta")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::MapArray>()
        .unwrap();
    assert!(meta.is_valid(split));
    assert_eq!(meta.value_length(split), 0);
}

#[test]
fn rows_are_written_in_the_given_order_across_row_groups() {
    let config = ParquetConfig {
        max_row_group_rows: NonZeroUsize::new(2).unwrap(),
        ..ParquetConfig::default()
    };
    let rows = state_rows();
    let writer = FileWriter::new(config).unwrap();
    let read = read(&writer.states.encode(&rows).unwrap());
    assert_eq!(read.metadata.num_row_groups(), 2);
    let all = concat_batches(&writer.states.schema(), &read.batches).unwrap();
    assert_eq!(all, writer.states.record_batch(&rows).unwrap());
}

#[test]
fn large_batches_are_converted_in_slices_without_losing_rows() {
    let mut row = state_rows().remove(0);
    let StateEntry::State { payload, .. } = &mut row.entry else {
        unreachable!("the first row is a state");
    };
    *payload = vec![b'x'; 1024 * 1024];
    // 70 MiB of payload crosses the 64 MiB slice size once.
    let rows: Vec<StateRow> = (0..70u64)
        .map(|ms| StateRow {
            time: UnixMillis::from(ms),
            ..row.clone()
        })
        .collect();
    let writer = writer();
    let read = read(&writer.states.encode(&rows).unwrap());
    let all = concat_batches(&writer.states.schema(), &read.batches).unwrap();
    assert_eq!(all, writer.states.record_batch(&rows).unwrap());
}

#[test]
fn states_schema_matches_the_spec() {
    let read = read(&states_file(&state_rows()));
    let schema = read.metadata.file_metadata().schema_descr();
    let expect = |name: &str, physical: PhysicalType, logical: Option<LogicalType>, repetition| {
        let column = schema.column(column_index(&read.metadata, name));
        assert_eq!(column.physical_type(), physical, "{name}");
        assert_eq!(column.logical_type_ref(), logical.as_ref(), "{name}");
        assert_eq!(
            column.self_type().get_basic_info().repetition(),
            repetition,
            "{name}"
        );
    };
    let uint32 = || Some(LogicalType::integer(32, false));
    let string = || Some(LogicalType::String);
    let millis_utc = Some(LogicalType::timestamp(true, TimeUnit::MILLIS));
    expect(
        "tree",
        PhysicalType::FIXED_LEN_BYTE_ARRAY,
        None,
        Repetition::REQUIRED,
    );
    expect(
        "subflow",
        PhysicalType::INT32,
        uint32(),
        Repetition::REQUIRED,
    );
    expect("ts", PhysicalType::INT64, millis_utc, Repetition::REQUIRED);
    expect(
        "kind",
        PhysicalType::BYTE_ARRAY,
        string(),
        Repetition::REQUIRED,
    );
    expect(
        "service",
        PhysicalType::BYTE_ARRAY,
        string(),
        Repetition::REQUIRED,
    );
    expect(
        "category",
        PhysicalType::BYTE_ARRAY,
        string(),
        Repetition::REQUIRED,
    );
    expect(
        "child_subflow",
        PhysicalType::INT32,
        uint32(),
        Repetition::OPTIONAL,
    );
    expect(
        "meta.key_value.key",
        PhysicalType::BYTE_ARRAY,
        string(),
        Repetition::REQUIRED,
    );
    expect(
        "meta.key_value.value",
        PhysicalType::BYTE_ARRAY,
        string(),
        Repetition::REQUIRED,
    );
    expect(
        "payload_type",
        PhysicalType::BYTE_ARRAY,
        string(),
        Repetition::OPTIONAL,
    );
    expect(
        "payload",
        PhysicalType::BYTE_ARRAY,
        None,
        Repetition::OPTIONAL,
    );
    assert_eq!(
        schema
            .column(column_index(&read.metadata, "tree"))
            .type_length(),
        12
    );
}

#[test]
fn refs_schema_matches_the_spec() {
    let read = read(&refs_file(&ref_rows()));
    let schema = read.metadata.file_metadata().schema_descr();
    let names: Vec<String> = schema.columns().iter().map(|c| c.path().string()).collect();
    assert_eq!(
        names,
        [
            "ref_type",
            "domain_id",
            "service",
            "category",
            "tree",
            "subflow",
            "ts"
        ]
    );
    let category = schema.column(column_index(&read.metadata, "category"));
    assert_eq!(
        category.self_type().get_basic_info().repetition(),
        Repetition::OPTIONAL
    );
    let domain_id = schema.column(column_index(&read.metadata, "domain_id"));
    assert_eq!(
        domain_id.self_type().get_basic_info().repetition(),
        Repetition::REQUIRED
    );
}

#[test]
fn files_are_zstd_without_bloom_filters() {
    let read = read(&states_file(&state_rows()));
    for column in read.metadata.row_group(0).columns() {
        assert!(
            matches!(column.compression(), Compression::ZSTD(_)),
            "{}",
            column.column_path()
        );
        assert_eq!(
            column.bloom_filter_offset(),
            None,
            "{}",
            column.column_path()
        );
    }
}

#[test]
fn payload_has_no_statistics_and_sort_columns_have_a_page_index() {
    let read = read(&states_file(&state_rows()));
    let row_group = read.metadata.row_group(0);
    assert!(
        row_group
            .column(column_index(&read.metadata, "payload"))
            .statistics()
            .is_none()
    );
    for name in ["tree", "subflow", "ts", "service"] {
        assert!(
            row_group
                .column(column_index(&read.metadata, name))
                .statistics()
                .is_some(),
            "{name}"
        );
    }
    let page_index = read.metadata.page_index_for_row_group(0);
    for name in ["tree", "subflow", "ts"] {
        assert!(
            page_index
                .column_index(column_index(&read.metadata, name))
                .is_some(),
            "{name}"
        );
    }
    assert!(
        page_index
            .column_index(column_index(&read.metadata, "payload"))
            .is_none()
    );
}

#[test]
fn other_columns_have_row_group_statistics_only() {
    let read = read(&states_file(&state_rows()));
    let row_group = read.metadata.row_group(0);
    let page_index = read.metadata.page_index_for_row_group(0);
    for name in [
        "kind",
        "service",
        "category",
        "child_subflow",
        "payload_type",
    ] {
        let index = column_index(&read.metadata, name);
        assert!(row_group.column(index).statistics().is_some(), "{name}");
        assert!(page_index.column_index(index).is_none(), "{name}");
    }
}

#[test]
fn payload_is_not_dictionary_encoded() {
    let read = read(&states_file(&state_rows()));
    let payload = read
        .metadata
        .row_group(0)
        .column(column_index(&read.metadata, "payload"));
    assert_eq!(payload.dictionary_page_offset(), None);
}

#[test]
fn subflow_statistics_compare_unsigned() {
    let mut rows = state_rows();
    rows[0].subflow = Subflow::from(0x8000_0001);
    rows[2].subflow = Subflow::from(1);
    let read = read(&states_file(&rows));
    let statistics = read
        .metadata
        .row_group(0)
        .column(column_index(&read.metadata, "subflow"))
        .statistics()
        .unwrap();
    let Statistics::Int32(values) = statistics else {
        panic!("expected INT32 statistics, got {statistics:?}");
    };
    assert_eq!(values.min_opt().map(|min| *min as u32), Some(0));
    assert_eq!(values.max_opt().map(|max| *max as u32), Some(0x8000_0001));
}

#[test]
fn time_past_year_9999_is_stored_as_its_end() {
    let mut rows = ref_rows();
    rows[0].time = UnixMillis::from(u64::MAX);
    let read = read(&refs_file(&rows));
    let ts = read.batches[0]
        .column_by_name("ts")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
        .unwrap()
        .value(0);
    assert_eq!(ts, jiff::Timestamp::MAX.as_millisecond());
}

/// A table that marks a nested column as a sort key.
struct NestedSortKey;

impl Table for NestedSortKey {
    fn columns() -> Vec<Column<Self>> {
        let list = arrow_schema::DataType::List(std::sync::Arc::new(
            arrow_schema::Field::new_list_field(arrow_schema::DataType::Utf8, true),
        ));
        vec![Column::required("tags", list, |_| unreachable!("never encoded")).sort_key()]
    }

    fn approximate_bytes(&self) -> usize {
        0
    }
}

#[test]
fn rejects_a_nested_sort_key() {
    let compression = Compression::ZSTD(Default::default());
    let layout = FileLayout::<NestedSortKey>::new(&ParquetConfig::default(), compression);
    assert!(matches!(layout, Err(FileError::Parquet(_))));
}

#[test]
fn row_groups_record_the_sort_order() {
    let states = read(&states_file(&state_rows()));
    let ascending = |column_idx| SortingColumn {
        column_idx,
        descending: false,
        nulls_first: false,
    };
    assert_eq!(
        states.metadata.row_group(0).sorting_columns(),
        Some(&vec![ascending(0), ascending(1), ascending(2)])
    );
    let refs = read(&refs_file(&ref_rows()));
    assert_eq!(
        refs.metadata.row_group(0).sorting_columns(),
        Some(&vec![ascending(0), ascending(1)])
    );
}

#[test]
fn files_record_schema_version_and_writer() {
    let read = read(&refs_file(&ref_rows()));
    let metadata: BTreeMap<&str, Option<&str>> = read
        .metadata
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .iter()
        .map(|kv| (kv.key.as_str(), kv.value.as_deref()))
        .collect();
    assert_eq!(metadata["burgess.schema_version"], Some("1"));
    assert!(
        metadata["burgess.writer"]
            .unwrap()
            .starts_with("burgess-writer ")
    );
}

#[test]
fn empty_batch_gives_two_valid_zero_row_files() {
    let files = writer().write_batch(&SortedBatch::default()).unwrap();
    for (file, schema) in [
        (&files.states, writer().states.schema()),
        (&files.refs, writer().refs.schema()),
    ] {
        assert_eq!(file.rows, 0);
        let read = read(file);
        assert_eq!(read.metadata.file_metadata().num_rows(), 0);
        assert!(read.batches.is_empty());
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file.bytes.clone())).unwrap();
        assert_eq!(builder.schema().fields(), schema.fields());
    }
}

#[test]
fn write_batch_writes_both_sets() {
    let batch = SortedBatch {
        states: state_rows(),
        refs: ref_rows(),
        ..SortedBatch::default()
    };
    let files = writer().write_batch(&batch).unwrap();
    assert_eq!((files.states.rows, files.refs.rows), (3, 2));
}

#[test]
fn rejects_invalid_compression_level() {
    let config = ParquetConfig {
        zstd_level: 99,
        ..ParquetConfig::default()
    };
    assert!(matches!(
        FileWriter::new(config),
        Err(FileError::Parquet(_))
    ));
}

/// Writes sample files for checking by hand, for example with
/// `duckdb -c "select * from '<dir>/states.parquet'"`.
#[test]
#[ignore = "writes files into BURGESS_SAMPLE_DIR for manual inspection"]
fn write_samples() {
    let dir = std::env::var("BURGESS_SAMPLE_DIR").expect("set BURGESS_SAMPLE_DIR");
    let batch = SortedBatch {
        states: state_rows(),
        refs: ref_rows(),
        ..SortedBatch::default()
    };
    let files = writer().write_batch(&batch).unwrap();
    std::fs::write(format!("{dir}/states.parquet"), files.states.bytes).unwrap();
    std::fs::write(format!("{dir}/refs.parquet"), files.refs.bytes).unwrap();
    let empty = writer().write_batch(&SortedBatch::default()).unwrap();
    std::fs::write(format!("{dir}/states-empty.parquet"), empty.states.bytes).unwrap();
}
