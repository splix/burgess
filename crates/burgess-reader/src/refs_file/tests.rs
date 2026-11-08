//! Reading references from refs files that the writer wrote: the right
//! rows, and only the pages that can hold them.

use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::{Day, FileStats, ObjectName, RefRow};
use burgess_core::wire::UnixMillis;
use burgess_writer::batch::SortedBatch;
use burgess_writer::parquet_files::{FileWriter, ParquetConfig};
use bytes::Bytes;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

use super::*;
use crate::parquet_file::{FileProblem, FileReadConfig};
use crate::test_support::RecordingStore;

const FIRST_MS: u64 = 1_793_000_000_000;

fn reference(ref_type: &str, domain_id: &str, i: usize) -> RefRow {
    RefRow {
        ref_type: ref_type.into(),
        domain_id: domain_id.into(),
        service: format!("service-{}", i % 3),
        category: i.is_multiple_of(2).then(|| "step".to_string()),
        tree: TreeId::from([i as u8; 12]),
        subflow: Subflow::from(i as u32),
        time: UnixMillis::from(FIRST_MS + i as u64),
    }
}

/// `count` references to each of `ids`, as `(ref_type, domain_id)`.
fn references(ids: &[(&str, &str)], count: usize) -> Vec<RefRow> {
    ids.iter()
        .flat_map(|&(ref_type, domain_id)| {
            (0..count).map(move |i| reference(ref_type, domain_id, i))
        })
        .collect()
}

fn key(ref_type: &str, domain_id: &str) -> RefKey {
    RefKey::new(ref_type.into(), domain_id.into()).unwrap()
}

/// One refs file in a store, and the rows it was written from.
struct Fixture {
    store: Arc<RecordingStore>,
    name: ObjectName,
    bytes: Bytes,
    /// In file order.
    rows: Vec<RefRow>,
}

impl Fixture {
    /// A file of `rows`, with pages as small as the writer makes them, so
    /// that each id spans several pages.
    async fn new(mut rows: Vec<RefRow>, max_row_group_rows: usize) -> Self {
        rows.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        let writer = FileWriter::new(ParquetConfig {
            max_row_group_rows: NonZeroUsize::new(max_row_group_rows).unwrap(),
            data_page_bytes: NonZeroUsize::new(1).unwrap(),
            ..ParquetConfig::default()
        })
        .unwrap();
        let batch = SortedBatch {
            refs: rows.clone(),
            ..SortedBatch::default()
        };
        let bytes = Bytes::from(writer.write_batch(&batch).unwrap().refs.bytes);
        let store = Arc::new(RecordingStore::default());
        let name = ObjectName::RefsFile {
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

    /// The end of the last data page. A first request of the bytes after it
    /// shows each data page fetched later as its own range.
    fn data_end(&self) -> u64 {
        self.metadata()
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

    async fn read_with(
        &self,
        config: FileReadConfig,
        key: &RefKey,
    ) -> Result<Vec<RefRow>, RefsReadError> {
        RefsFileReader::new(self.store.clone(), config)
            .refs(&self.name, self.stats(), key)
            .await
    }

    async fn read(&self, key: &RefKey) -> Vec<RefRow> {
        let tail = self.bytes.len() - self.data_end() as usize;
        let config = FileReadConfig {
            refs_tail_bytes: NonZeroUsize::new(tail).unwrap(),
            ..FileReadConfig::default()
        };
        self.read_with(config, key).await.unwrap()
    }

    fn expected(&self, key: &RefKey) -> Vec<RefRow> {
        self.rows
            .iter()
            .filter(|row| row.sort_key() == (key.ref_type(), key.domain_id()))
            .cloned()
            .collect()
    }
}

#[tokio::test]
async fn returns_every_reference_to_the_id_and_no_other() {
    let ids = [
        ("order_id", "ORD-1"),
        ("order_id", "ORD-2"),
        ("order_id", "ORD-3"),
        ("tx_hash", "ORD-2"),
    ];
    let fixture = Fixture::new(references(&ids, 200), 150).await;
    assert!(fixture.metadata().num_row_groups() > 1);

    for (ref_type, domain_id) in ids {
        let key = key(ref_type, domain_id);
        let rows = fixture.read(&key).await;
        assert_eq!(rows.len(), 200, "{ref_type} {domain_id}");
        assert_eq!(rows, fixture.expected(&key));
    }
}

#[tokio::test]
async fn one_id_gives_a_flat_row_for_each_flow() {
    let fixture = Fixture::new(references(&[("order_id", "ORD-1")], 3), 1 << 20).await;

    let rows = fixture.read(&key("order_id", "ORD-1")).await;

    let flows: Vec<(TreeId, u32)> = rows
        .iter()
        .map(|row| (row.tree, u32::from(row.subflow)))
        .collect();
    assert_eq!(
        flows,
        [
            (TreeId::from([0; 12]), 0),
            (TreeId::from([1; 12]), 1),
            (TreeId::from([2; 12]), 2)
        ]
    );
    assert_eq!(rows[0].category.as_deref(), Some("step"));
    assert_eq!(rows[1].category, None);
}

#[tokio::test]
async fn the_lookup_is_case_sensitive() {
    let fixture = Fixture::new(references(&[("order_id", "ORD-1")], 10), 1 << 20).await;

    assert_eq!(fixture.read(&key("order_id", "ord-1")).await, []);
    assert_eq!(fixture.read(&key("Order_id", "ORD-1")).await, []);
}

#[tokio::test]
async fn an_id_that_the_bounds_rule_out_reads_no_data_page() {
    let ids = [("order_id", "ORD-2"), ("order_id", "ORD-4")];
    let fixture = Fixture::new(references(&ids, 300), 1 << 20).await;
    let missing = [
        key("order_id", "ORD-1"),
        key("order_id", "ORD-5"),
        key("aaa", "ORD-2"),
        key("zzz", "ORD-2"),
    ];

    for key in &missing {
        assert_eq!(fixture.read(key).await, [], "{key:?}");
    }

    assert_eq!(
        fixture.store.requests().len(),
        missing.len(),
        "one request per read"
    );
    let data_end = fixture.data_end();
    for range in fixture.store.ranges() {
        assert!(
            range.start >= data_end,
            "{range:?} reads data before {data_end}"
        );
    }
}

#[tokio::test]
async fn an_id_reads_only_the_pages_that_can_hold_it() {
    let ids = [
        ("order_id", "ORD-1"),
        ("order_id", "ORD-2"),
        ("order_id", "ORD-3"),
    ];
    // The writer closes a page after a write batch of rows, thus each id
    // needs a few batches to span pages of its own.
    let fixture = Fixture::new(references(&ids, 3000), 1 << 20).await;

    let rows = fixture.read(&key("order_id", "ORD-2")).await;

    assert_eq!(rows.len(), 3000);
    let data_end = fixture.data_end();
    let data_bytes: u64 = fixture
        .store
        .ranges()
        .iter()
        .filter(|range| range.start < data_end)
        .map(|range| range.end - range.start)
        .sum();
    assert!(
        data_bytes < data_end / 2,
        "{data_bytes} of {data_end} data bytes read for a third of the rows"
    );
}

#[tokio::test]
async fn an_id_longer_than_the_bounds_the_writer_keeps_is_found() {
    let long = |c: char| format!("{}{c}", "x".repeat(200));
    let ids = [
        ("order_id", long('a').as_str()),
        ("order_id", long('b').as_str()),
        ("order_id", long('c').as_str()),
    ]
    .map(|(ref_type, domain_id)| (ref_type, domain_id.to_string()));
    let ids: Vec<(&str, &str)> = ids
        .iter()
        .map(|(ref_type, domain_id)| (*ref_type, domain_id.as_str()))
        .collect();
    // Enough rows that each id has pages of its own, with bounds that the
    // writer truncated.
    let fixture = Fixture::new(references(&ids, 1500), 1 << 20).await;

    for (ref_type, domain_id) in &ids {
        let key = key(ref_type, domain_id);
        assert_eq!(fixture.read(&key).await, fixture.expected(&key));
    }
    assert_eq!(fixture.read(&key("order_id", &long('d'))).await, []);
}

#[tokio::test]
async fn a_file_with_other_rows_than_the_manifest_lists_is_refused() {
    let mut fixture = Fixture::new(references(&[("order_id", "ORD-1")], 10), 1 << 20).await;
    fixture.rows.pop();

    let error = fixture
        .read_with(FileReadConfig::default(), &key("order_id", "ORD-1"))
        .await
        .unwrap_err();

    assert_eq!(error.file(), &fixture.name);
    assert!(
        matches!(
            error.problem(),
            RefsFileProblem::File(FileProblem::RowCount {
                listed: 9,
                found: 10
            })
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_missing_file_is_a_store_problem() {
    let fixture = Fixture::new(references(&[("order_id", "ORD-1")], 10), 1 << 20).await;
    fixture
        .store
        .inner
        .delete(&Path::from(fixture.name.to_string()))
        .await
        .unwrap();

    let error = fixture
        .read_with(FileReadConfig::default(), &key("order_id", "ORD-1"))
        .await
        .unwrap_err();

    assert!(
        matches!(
            error.problem(),
            RefsFileProblem::File(FileProblem::Store(object_store::Error::NotFound { .. }))
        ),
        "{error:?}"
    );
}

#[test]
fn a_key_is_trimmed_and_checked_as_the_writer_checks_a_reference() {
    let key = key("  order_id ", " ORD-1\t");
    assert_eq!((key.ref_type(), key.domain_id()), ("order_id", "ORD-1"));

    assert!(RefKey::new("order id".into(), "ORD-1".into()).is_err());
    assert!(RefKey::new("order_id".into(), "  ".into()).is_err());
    assert!(RefKey::new("order_id".into(), "x".repeat(257)).is_err());
}
