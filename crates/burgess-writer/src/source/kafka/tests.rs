//! Runs against a real Kafka broker, started in Docker with testcontainers.
//!
//! Starting a broker takes seconds, so one test starts it once and runs every
//! scenario against it in turn, each on a topic of its own. The test needs
//! Docker and is ignored by default:
//! `cargo test -p burgess-writer -- --ignored`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use super::*;

const PATIENCE: Duration = Duration::from_secs(20);

fn up_to(max: usize) -> NonZeroUsize {
    NonZeroUsize::new(max).unwrap()
}

fn partition(id: u32) -> PartitionId {
    PartitionId::from(id)
}

/// A new topic with `partitions` partitions and a name no other test uses,
/// returned once the broker lists all its partitions.
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

    let listed = log(servers, &name).await;
    let visible = tokio::time::timeout(PATIENCE, async {
        loop {
            if let Ok(listed) = listed.partitions().await
                && listed.len() == partitions as usize
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(visible.is_ok(), "topic {name} did not appear");
    name
}

async fn produce(servers: &str, topic: &str, messages: &[(i32, &str)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", servers)
        .create()
        .unwrap();
    for (partition, payload) in messages {
        let record = FutureRecord::<(), str>::to(topic)
            .partition(*partition)
            .payload(payload);
        producer.send(record, PATIENCE).await.unwrap();
    }
}

async fn log(servers: &str, topic: &str) -> KafkaLog {
    let mut config = KafkaConfig::new(servers, topic);
    config.request_timeout = PATIENCE;
    KafkaLog::new(config).unwrap()
}

/// Pulls until `count` entries arrived, failing the test if they do not.
async fn pull_exactly(log: &mut KafkaLog, count: usize) -> Vec<LogEntry> {
    let mut entries = Vec::new();
    while entries.len() < count {
        let pulled = tokio::time::timeout(PATIENCE, log.pull(up_to(count - entries.len())))
            .await
            .expect("entries arrive in time")
            .unwrap();
        entries.extend(pulled);
    }
    entries
}

fn payloads(entries: &[LogEntry]) -> Vec<(u32, String)> {
    let mut payloads: Vec<(u32, String)> = entries
        .iter()
        .map(|entry| {
            (
                u32::from(entry.partition),
                String::from_utf8(entry.payload.clone()).unwrap(),
            )
        })
        .collect();
    payloads.sort();
    payloads
}

#[tokio::test]
#[ignore = "needs Docker: starts Kafka with testcontainers"]
async fn kafka_log_against_a_real_broker() {
    let kafka = Kafka::default()
        .start()
        .await
        .expect("Kafka starts in Docker");
    let port = kafka
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("Kafka port is mapped");
    let servers = format!("127.0.0.1:{port}");

    lists_the_topic_partitions(&servers).await;
    unknown_topic_is_an_error(&servers).await;
    first_run_reads_every_partition_from_the_start(&servers).await;
    resume_at_is_the_offset_after_the_entry(&servers).await;
    resuming_returns_exactly_the_entries_after_the_position(&servers).await;
    cancelled_pull_loses_nothing(&servers).await;
    pulls_cancelled_while_records_arrive_lose_nothing(&servers).await;
    pull_returns_at_most_max(&servers).await;
    unknown_partition_is_an_error(&servers).await;
    position_past_the_end_is_an_error(&servers).await;
    position_at_the_end_waits_for_new_records(&servers).await;
    position_before_the_retained_records_resumes_at_the_earliest(&servers).await;
}

async fn lists_the_topic_partitions(servers: &str) {
    let topic = topic(servers, 3).await;
    let partitions = log(servers, &topic).await.partitions().await.unwrap();
    assert_eq!(partitions, [partition(0), partition(1), partition(2)]);
}

async fn unknown_topic_is_an_error(servers: &str) {
    let log = log(servers, "burgess-test-no-such-topic").await;
    assert!(matches!(
        log.partitions().await,
        Err(KafkaLogError::UnknownTopic { .. })
    ));
}

async fn first_run_reads_every_partition_from_the_start(servers: &str) {
    let topic = topic(servers, 2).await;
    produce(servers, &topic, &[(0, "a0"), (1, "b0"), (0, "a1")]).await;

    let mut log = log(servers, &topic).await;
    log.resume(&BTreeMap::new()).await.unwrap();
    let entries = pull_exactly(&mut log, 3).await;
    assert_eq!(
        payloads(&entries),
        [(0, "a0".into()), (0, "a1".into()), (1, "b0".into())]
    );
}

async fn resume_at_is_the_offset_after_the_entry(servers: &str) {
    let topic = topic(servers, 1).await;
    produce(servers, &topic, &[(0, "x0"), (0, "x1")]).await;

    let mut log = log(servers, &topic).await;
    log.resume(&BTreeMap::new()).await.unwrap();
    let entries = pull_exactly(&mut log, 2).await;
    let positions: Vec<u64> = entries.iter().map(|entry| entry.resume_at.into()).collect();
    assert_eq!(positions, [1, 2]);
}

async fn resuming_returns_exactly_the_entries_after_the_position(servers: &str) {
    let topic = topic(servers, 2).await;
    produce(
        servers,
        &topic,
        &[(0, "a0"), (0, "a1"), (0, "a2"), (1, "b0"), (1, "b1")],
    )
    .await;

    let known = BTreeMap::from([
        (partition(0), Position::from(2)),
        (partition(1), Position::from(1)),
    ]);
    let mut log = log(servers, &topic).await;
    log.resume(&known).await.unwrap();
    let entries = pull_exactly(&mut log, 2).await;
    assert_eq!(payloads(&entries), [(0, "a2".into()), (1, "b1".into())]);
    let nothing_more = tokio::time::timeout(Duration::from_secs(2), log.pull(up_to(10))).await;
    assert!(nothing_more.is_err(), "no entries beyond the two expected");
}

async fn cancelled_pull_loses_nothing(servers: &str) {
    let topic = topic(servers, 1).await;
    let mut log = log(servers, &topic).await;
    log.resume(&BTreeMap::new()).await.unwrap();

    let cancelled = tokio::time::timeout(Duration::from_secs(2), log.pull(up_to(10))).await;
    assert!(cancelled.is_err());
    produce(servers, &topic, &[(0, "late")]).await;
    assert_eq!(
        payloads(&pull_exactly(&mut log, 1).await),
        [(0, "late".into())]
    );
}

async fn pull_returns_at_most_max(servers: &str) {
    let topic = topic(servers, 1).await;
    produce(servers, &topic, &[(0, "1"), (0, "2"), (0, "3"), (0, "4")]).await;
    let mut log = log(servers, &topic).await;
    log.resume(&BTreeMap::new()).await.unwrap();

    let mut sizes = Vec::new();
    let mut seen = 0;
    while seen < 4 {
        let pulled = tokio::time::timeout(PATIENCE, log.pull(up_to(2)))
            .await
            .unwrap()
            .unwrap();
        seen += pulled.len();
        sizes.push(pulled.len());
    }
    assert!(sizes.iter().all(|size| *size <= 2), "{sizes:?}");
}

async fn unknown_partition_is_an_error(servers: &str) {
    let topic = topic(servers, 1).await;
    let mut log = log(servers, &topic).await;
    let assigned = log
        .assign(BTreeMap::from([(partition(5), Start::Earliest)]))
        .await;
    assert!(matches!(
        assigned,
        Err(KafkaLogError::UnknownPartition(id)) if id == partition(5)
    ));
}

async fn position_past_the_end_is_an_error(servers: &str) {
    let topic = topic(servers, 1).await;
    produce(servers, &topic, &[(0, "only")]).await;
    let mut log = log(servers, &topic).await;
    let assigned = log
        .assign(BTreeMap::from([(
            partition(0),
            Start::At(Position::from(5)),
        )]))
        .await;
    assert!(matches!(
        assigned,
        Err(KafkaLogError::PositionOutOfRange { end: 1, .. })
    ));
}

async fn position_at_the_end_waits_for_new_records(servers: &str) {
    let topic = topic(servers, 1).await;
    produce(servers, &topic, &[(0, "old")]).await;
    let mut log = log(servers, &topic).await;
    log.assign(BTreeMap::from([(
        partition(0),
        Start::At(Position::from(1)),
    )]))
    .await
    .unwrap();
    produce(servers, &topic, &[(0, "new")]).await;
    assert_eq!(
        payloads(&pull_exactly(&mut log, 1).await),
        [(0, "new".into())]
    );
}

async fn position_before_the_retained_records_resumes_at_the_earliest(servers: &str) {
    let topic = topic(servers, 1).await;
    produce(
        servers,
        &topic,
        &[(0, "r0"), (0, "r1"), (0, "r2"), (0, "r3")],
    )
    .await;

    // Deleting records is what retention does to a writer that was away.
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", servers)
        .create()
        .unwrap();
    let mut before = TopicPartitionList::new();
    before
        .add_partition_offset(&topic, 0, Offset::Offset(2))
        .unwrap();
    admin
        .delete_records(&before, &AdminOptions::new())
        .await
        .unwrap();

    let mut log = log(servers, &topic).await;
    log.assign(BTreeMap::from([(
        partition(0),
        Start::At(Position::from(1)),
    )]))
    .await
    .unwrap();
    let entries = pull_exactly(&mut log, 2).await;
    assert_eq!(payloads(&entries), [(0, "r2".into()), (0, "r3".into())]);
}

/// Pulls with a zero timeout are cancelled whenever they would wait, which
/// happens again and again while librdkafka is still fetching. Every record
/// must still arrive exactly once, in order.
async fn pulls_cancelled_while_records_arrive_lose_nothing(servers: &str) {
    let topic = topic(servers, 1).await;
    let sent: Vec<String> = (0..50).map(|i| format!("m{i:02}")).collect();
    let messages: Vec<(i32, &str)> = sent.iter().map(|payload| (0, payload.as_str())).collect();
    produce(servers, &topic, &messages).await;

    let mut log = log(servers, &topic).await;
    log.resume(&BTreeMap::new()).await.unwrap();
    let mut received = Vec::new();
    let mut cancelled = 0;
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while received.len() < sent.len() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "records arrive in time"
        );
        match tokio::time::timeout(Duration::ZERO, log.pull(up_to(3))).await {
            Ok(pulled) => received.extend(pulled.unwrap()),
            Err(_) => {
                cancelled += 1;
                tokio::task::yield_now().await;
            }
        }
    }
    let payloads: Vec<String> = received
        .iter()
        .map(|entry| String::from_utf8(entry.payload.clone()).unwrap())
        .collect();
    assert_eq!(payloads, sent);
    assert!(cancelled > 0, "some pulls were cancelled");
}
