//! The sink against an unreachable broker, and against a real one started
//! in Docker with testcontainers.
//!
//! Starting a broker takes seconds, so one test starts it once and runs every
//! scenario against it in turn, each on a topic of its own. That test needs
//! Docker and is ignored by default:
//! `cargo test -p burgess-client -- --ignored`.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use burgess_core::flow::FlowId;
use burgess_core::wire::{Body, Record};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as _;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde_json::json;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use super::*;
use crate::emitter::Emitter;

const PATIENCE: Duration = Duration::from_secs(20);

/// One message as the writer would read it.
#[derive(Debug)]
struct Consumed {
    partition: i32,
    key: Vec<u8>,
    record: Record,
}

fn emitter(servers: &str, topic: &str, compression: Compression) -> Emitter<KafkaSink> {
    let mut config = KafkaConfig::new(servers, topic);
    config.compression = compression;
    Emitter::new("checkout", KafkaSink::new(&config).unwrap())
}

#[test]
fn send_and_flush_return_while_kafka_is_down() {
    let emitter = emitter("127.0.0.1:1", "records", Compression::default());
    let started = Instant::now();
    let root = emitter.start();
    emitter.capture(root, "payment", &json!({"amount": 42}));
    emitter.flush(Duration::from_millis(200));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

/// A new topic with `partitions` partitions and a name no other scenario
/// uses, returned once the broker lists all its partitions.
async fn topic(servers: &str, partitions: i32) -> String {
    static CREATED: AtomicUsize = AtomicUsize::new(0);
    let name = format!("scenario-{}", CREATED.fetch_add(1, Ordering::Relaxed));
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", servers)
        .create()
        .unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new(&name, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(PATIENCE)),
        )
        .await
        .unwrap();
    assert!(created.iter().all(Result::is_ok), "{created:?}");

    let consumer = consumer(servers);
    let deadline = Instant::now() + PATIENCE;
    loop {
        let listed = consumer
            .fetch_metadata(Some(&name), PATIENCE)
            .map(|metadata| metadata.topics()[0].partitions().len())
            .unwrap_or(0);
        if listed == partitions as usize {
            return name;
        }
        assert!(Instant::now() < deadline, "topic {name} did not appear");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn consumer(servers: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", servers)
        .set("group.id", "burgess-client-test")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap()
}

/// Reads `count` messages from every partition of the topic, failing the
/// test if they do not arrive.
fn consume_exactly(servers: &str, topic: &str, partitions: i32, count: usize) -> Vec<Consumed> {
    let consumer = consumer(servers);
    let mut assignment = TopicPartitionList::new();
    for partition in 0..partitions {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .unwrap();
    }
    consumer.assign(&assignment).unwrap();

    let deadline = Instant::now() + PATIENCE;
    let mut consumed = Vec::new();
    while consumed.len() < count {
        assert!(
            Instant::now() < deadline,
            "only {} messages arrived",
            consumed.len()
        );
        let Some(message) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        let message = message.unwrap();
        consumed.push(Consumed {
            partition: message.partition(),
            key: message.key().unwrap().to_vec(),
            record: Record::decode(message.payload().unwrap()).unwrap(),
        });
    }
    consumed
}

fn kind(body: &Body) -> &'static str {
    match body {
        Body::State(_) => "state",
        Body::Split(_) => "split",
        Body::Reference(_) => "reference",
    }
}

#[tokio::test]
#[ignore = "needs Docker: starts Kafka with testcontainers"]
async fn kafka_sink_against_a_real_broker() {
    let kafka = Kafka::default()
        .start()
        .await
        .expect("Kafka starts in Docker");
    let port = kafka
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("Kafka port is mapped");
    let servers = format!("127.0.0.1:{port}");

    delivers_every_record_keyed_by_its_tree(&servers).await;
    puts_each_tree_in_its_own_partition(&servers).await;
    every_compression_reaches_the_reader(&servers).await;
}

async fn delivers_every_record_keyed_by_its_tree(servers: &str) {
    let topic = topic(servers, 1).await;
    let emitter = emitter(servers, &topic, Compression::default());
    let root = emitter.start();
    let child = emitter.split(root, "fan-out");
    emitter.capture(child, "payment", &json!({"amount": 42}));
    emitter.reference(root, "order_id", "ORD-1");
    emitter.flush(PATIENCE);

    let consumed = consume_exactly(servers, &topic, 1, 3);
    for message in &consumed {
        assert_eq!(message.key, message.record.flow_id.tree().as_bytes());
    }
    let sent: BTreeSet<(FlowId, &str)> = consumed
        .iter()
        .map(|message| (message.record.flow_id, kind(&message.record.body)))
        .collect();
    assert_eq!(
        sent,
        BTreeSet::from([(root, "split"), (child, "state"), (root, "reference")])
    );
}

async fn puts_each_tree_in_its_own_partition(servers: &str) {
    const PARTITIONS: i32 = 4;
    const TREES: usize = 16;
    let topic = topic(servers, PARTITIONS).await;
    let emitter = emitter(servers, &topic, Compression::default());
    for _ in 0..TREES {
        let root = emitter.start();
        let child = emitter.split(root, "fan-out");
        emitter.capture(child, "payment", &json!({}));
    }
    emitter.flush(PATIENCE);

    let consumed = consume_exactly(servers, &topic, PARTITIONS, TREES * 2);
    let count = NonZeroU32::new(PARTITIONS as u32).unwrap();
    for message in &consumed {
        let expected = message.record.flow_id.tree().partition(count);
        assert_eq!(message.partition, expected as i32, "{message:?}");
    }
    let used: BTreeSet<i32> = consumed.iter().map(|message| message.partition).collect();
    assert!(used.len() > 1, "trees spread over partitions: {used:?}");
}

async fn every_compression_reaches_the_reader(servers: &str) {
    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let topic = topic(servers, 1).await;
        let emitter = emitter(servers, &topic, compression);
        let root = emitter.start();
        emitter.capture(
            root,
            "payment",
            &json!({"codec": format!("{compression:?}")}),
        );
        emitter.flush(PATIENCE);

        let consumed = consume_exactly(servers, &topic, 1, 1);
        assert_eq!(consumed[0].record.flow_id, root, "{compression:?}");
    }
}
