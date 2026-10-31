//! Where and how the sink produces.

use std::collections::BTreeMap;

use rdkafka::ClientConfig;

use super::compression::Compression;
use super::error::KafkaSinkError;

/// librdkafka names the sink sets from typed fields, including older
/// aliases. The passthrough map must not reach them, or which value wins
/// would depend on the order librdkafka applies them in.
const RESERVED_NAMES: [&str; 4] = [
    "bootstrap.servers",
    "metadata.broker.list",
    "compression.type",
    "compression.codec",
];

/// librdkafka settings the sink sets unless a deployment overrides them
/// through [`KafkaConfig::properties`].
const DEFAULT_PROPERTIES: [(&str, &str); 2] = [
    // Already librdkafka's default, but stating it matters: the sink's
    // partitioner is attached to librdkafka's default topic config, which
    // librdkafka creates only once a topic-level property is set. Without
    // one, rdkafka attaches it to a null pointer and the process crashes on
    // the first send. Overriding `acks` keeps the config in place.
    ("acks", "all"),
    // While Kafka is out of reach, librdkafka queues records inside the
    // application the sink observes, up to 100,000 records or 1 GiB,
    // whichever fills first. Small records hit the record limit long before
    // the byte limit, but large states would reach the full gigabyte, so the
    // byte limit is cut to 64 MiB; that still rides out a short outage. Past
    // either limit records are dropped rather than risking the application's
    // memory. Raising this alone does not help small records: raise
    // `queue.buffering.max.messages` too.
    ("queue.buffering.max.kbytes", "65536"),
];

/// Where the records go.
///
/// A plain struct rather than a config file or environment variables: the
/// application embedding the sink already has its own configuration, and
/// fills this in from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaConfig {
    /// Brokers to connect to first, `host:port` separated by commas.
    pub bootstrap_servers: String,
    /// The one topic all producers write to and the writer reads.
    pub topic: String,
    /// How batches are compressed.
    pub compression: Compression,
    /// Further librdkafka settings passed through as they are, for what
    /// differs between deployments: client id, buffering and timeouts.
    ///
    /// TLS and SASL settings are refused when the sink is created: librdkafka
    /// is built without OpenSSL until a deployment needs it.
    pub properties: BTreeMap<String, String>,
}

impl KafkaConfig {
    /// A config with zstd compression and no extra properties.
    pub fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        KafkaConfig {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            compression: Compression::default(),
            properties: BTreeMap::new(),
        }
    }

    pub(super) fn client_config(&self) -> Result<ClientConfig, KafkaSinkError> {
        let mut config = ClientConfig::new();
        for (key, value) in DEFAULT_PROPERTIES {
            config.set(key, value);
        }
        for (key, value) in &self.properties {
            if RESERVED_NAMES.contains(&key.as_str()) {
                return Err(KafkaSinkError::ReservedProperty { key: key.clone() });
            }
            config.set(key, value);
        }
        config.set("bootstrap.servers", &self.bootstrap_servers);
        config.set("compression.type", self.compression.property());
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compresses_with_zstd_by_default() {
        let client = KafkaConfig::new("broker:9092", "records")
            .client_config()
            .unwrap();
        assert_eq!(client.get("compression.type"), Some("zstd"));
    }

    #[test]
    fn sets_the_chosen_compression() {
        let mut config = KafkaConfig::new("broker:9092", "records");
        config.compression = Compression::Lz4;
        let client = config.client_config().unwrap();
        assert_eq!(client.get("compression.type"), Some("lz4"));
    }

    #[test]
    fn passes_extra_properties_through() {
        let mut config = KafkaConfig::new("broker:9092", "records");
        config
            .properties
            .insert("client.id".into(), "checkout-1".into());
        let client = config.client_config().unwrap();
        assert_eq!(client.get("client.id"), Some("checkout-1"));
        assert_eq!(client.get("bootstrap.servers"), Some("broker:9092"));
    }

    #[test]
    fn buffers_at_most_64_mib_by_default() {
        let client = KafkaConfig::new("broker:9092", "records")
            .client_config()
            .unwrap();
        assert_eq!(client.get("queue.buffering.max.kbytes"), Some("65536"));
    }

    #[test]
    fn sets_a_topic_level_property_for_the_partitioner() {
        let client = KafkaConfig::new("broker:9092", "records")
            .client_config()
            .unwrap();
        assert_eq!(
            client.get("acks"),
            Some("all"),
            "without a topic-level property the producer crashes on its first send"
        );
    }

    #[test]
    fn buffer_limit_can_be_overridden() {
        let mut config = KafkaConfig::new("broker:9092", "records");
        config
            .properties
            .insert("queue.buffering.max.kbytes".into(), "262144".into());
        let client = config.client_config().unwrap();
        assert_eq!(client.get("queue.buffering.max.kbytes"), Some("262144"));
    }

    #[test]
    fn refuses_to_override_typed_settings() {
        for key in RESERVED_NAMES {
            let mut config = KafkaConfig::new("broker:9092", "records");
            config.properties.insert(key.into(), "anything".into());
            assert!(
                matches!(
                    config.client_config(),
                    Err(KafkaSinkError::ReservedProperty { key: refused }) if refused == key
                ),
                "{key}"
            );
        }
    }
}
