//! Records travel from an application to Parquet files: the client library
//! produces to a real Kafka broker, started in Docker with testcontainers,
//! and the writer stores what it reads in a local directory.
//!
//! A smoke test of the whole path rather than of any one part, which have
//! tests of their own. It needs Docker and is ignored by default:
//! `cargo test -p burgess-writer --test end_to_end -- --ignored`.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arrow_array::{Array, FixedSizeBinaryArray};
use burgess_client::{Emitter, KafkaSink, TreeId};
use burgess_core::storage::{Day, DayManifest};
use burgess_core::validation::Limits;
use burgess_writer::batch::BatchConfig;
use burgess_writer::config::{Settings, StoreConfig};
use burgess_writer::flush::BloomConfig;
use burgess_writer::parquet_files::ParquetConfig;
use burgess_writer::source::KafkaConfig;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use serde_json::json;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::watch;

const PATIENCE: Duration = Duration::from_secs(30);
const TOPIC: &str = "records";
const PARTITIONS: i32 = 3;
const TREES: usize = 20;
/// Each tree has two splits and three states, all stored as states rows.
const STATE_ROWS_PER_TREE: u64 = 5;
/// Each tree has one reference.
const REF_ROWS_PER_TREE: u64 = 1;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker: starts Kafka with testcontainers"]
async fn records_reach_parquet_files() {
    let kafka = Kafka::default()
        .start()
        .await
        .expect("Kafka starts in Docker");
    let port = kafka
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("Kafka port is mapped");
    let servers = format!("127.0.0.1:{port}");
    create_topic(&servers).await;

    let trees = emit_trees(&servers);

    let store = tempfile::tempdir().unwrap();
    let (stop, stopped) = watch::channel(false);
    let writer = tokio::spawn(burgess_writer::app::run(
        settings(&servers, store.path()),
        stopped,
    ));
    let expected = (
        TREES as u64 * STATE_ROWS_PER_TREE,
        TREES as u64 * REF_ROWS_PER_TREE,
    );
    let deadline = Instant::now() + PATIENCE;
    while stored_rows(store.path()) != expected {
        assert!(
            Instant::now() < deadline,
            "stored {:?} rows, expected {expected:?}",
            stored_rows(store.path())
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.send(true).unwrap();
    writer.await.unwrap().expect("writer stops cleanly");

    // Stopping stores the batch read so far, so this also checks nothing
    // was stored twice.
    assert_eq!(stored_rows(store.path()), expected);
    assert_eq!(stored_trees(store.path()), trees);
}

/// Emits [`TREES`] trees through the client, returning their ids.
fn emit_trees(servers: &str) -> BTreeSet<TreeId> {
    let sink = KafkaSink::new(&burgess_client::KafkaConfig::new(servers, TOPIC)).unwrap();
    let emitter = Emitter::new("checkout", sink);
    let trees = (0..TREES)
        .map(|order| {
            let root = emitter.start();
            let payment = emitter.split(root, "payment");
            let shipping = emitter.split(root, "shipping");
            emitter.capture(root, "order", json!({"order": order}));
            emitter.capture(payment, "charge", json!({"amount": 42}));
            emitter.capture(shipping, "label", json!({"carrier": "post"}));
            emitter.reference(root, "order_id", format!("ORD-{order}"));
            root.tree()
        })
        .collect();
    emitter.flush(PATIENCE);
    trees
}

fn settings(servers: &str, store: &Path) -> Settings {
    let mut kafka = KafkaConfig::new(servers, TOPIC);
    kafka.request_timeout = PATIENCE;
    Settings {
        writer_id: "smoke-writer".parse().unwrap(),
        kafka,
        store: StoreConfig::Local {
            path: store.to_path_buf(),
        },
        // Short batches, so the files appear while the test waits.
        batch: BatchConfig {
            max_interval: Duration::from_secs(1),
            max_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
        },
        limits: Limits::default(),
        parquet: ParquetConfig::default(),
        bloom: BloomConfig::default(),
    }
}

/// Creates the topic and waits until the broker lists all its partitions:
/// the writer refuses to start on a topic it cannot see.
async fn create_topic(servers: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", servers)
        .create()
        .unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new(TOPIC, PARTITIONS, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(PATIENCE)),
        )
        .await
        .unwrap();
    assert!(created.iter().all(Result::is_ok), "{created:?}");

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", servers)
        .create()
        .unwrap();
    let deadline = Instant::now() + PATIENCE;
    loop {
        let listed = consumer
            .fetch_metadata(Some(TOPIC), PATIENCE)
            .map(|metadata| metadata.topics()[0].partitions().len())
            .unwrap_or(0);
        if listed == PARTITIONS as usize {
            return;
        }
        assert!(Instant::now() < deadline, "topic did not appear");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The files of every day directory with the given name suffix.
fn day_files(store: &Path, suffix: &str) -> Vec<PathBuf> {
    let Ok(days) = fs::read_dir(store) else {
        return Vec::new();
    };
    days.flatten()
        // The checkpoint folder is next to the days.
        .filter(|day| day.file_name().to_string_lossy().parse::<Day>().is_ok())
        .flat_map(|day| fs::read_dir(day.path()).into_iter().flatten().flatten())
        .map(|file| file.path())
        .filter(|path| path.to_string_lossy().ends_with(suffix))
        .collect()
}

/// States and refs rows stored, as the day manifests count them.
fn stored_rows(store: &Path) -> (u64, u64) {
    day_files(store, "/manifest")
        .iter()
        .map(|path| DayManifest::decode(&fs::read(path).unwrap()).unwrap())
        .flat_map(|manifest| manifest.entries)
        .fold((0, 0), |(states, refs), entry| {
            (states + entry.states.rows, refs + entry.refs.rows)
        })
}

/// The trees found in the states files.
fn stored_trees(store: &Path) -> BTreeSet<TreeId> {
    let mut trees = BTreeSet::new();
    for path in day_files(store, "-states.parquet") {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let column = batch.column_by_name("tree").expect("tree column");
            let column = column
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("tree ids are fixed-size binary");
            for row in 0..column.len() {
                let bytes: [u8; 12] = column.value(row).try_into().unwrap();
                trees.insert(TreeId::from(bytes));
            }
        }
    }
    trees
}
