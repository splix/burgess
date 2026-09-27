//! How to reach the Kafka topic.

use std::collections::BTreeMap;
use std::time::Duration;

use rdkafka::ClientConfig;

use super::error::KafkaLogError;

/// librdkafka settings the source sets itself. They make the checkpoint the
/// only position store and turn lost positions into errors, so a deployment
/// must not override them.
const FIXED_PROPERTIES: [(&str, &str); 5] = [
    // librdkafka refuses to consume without a group id, even though the
    // writer never subscribes, so it never joins the group; with commits off
    // nothing is ever stored under it. Offsets live in the checkpoint only.
    ("group.id", "burgess-writer"),
    ("enable.auto.commit", "false"),
    ("enable.auto.offset.store", "false"),
    // Records aging out while being read must not be skipped silently.
    ("auto.offset.reset", "error"),
    ("enable.partition.eof", "false"),
];

/// Further names that would set the same things as the fixed properties:
/// the broker list, and librdkafka's older aliases of fixed settings. The
/// passthrough map must not reach them either, or which value wins would
/// depend on the order librdkafka applies them in.
const RESERVED_NAMES: [&str; 3] = [
    "bootstrap.servers",
    "metadata.broker.list",
    "auto.commit.enable",
];

/// Where the records come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaConfig {
    /// Brokers to connect to first, `host:port` separated by commas.
    pub bootstrap_servers: String,
    /// The one topic all producers write to.
    pub topic: String,
    /// Longest to wait for metadata and offset lookups.
    pub request_timeout: Duration,
    /// Further librdkafka settings passed through as they are, for what
    /// differs between deployments: TLS, SASL, client id, fetch tuning.
    pub properties: BTreeMap<String, String>,
}

impl KafkaConfig {
    /// A config with no extra properties and a 30 second request timeout.
    pub fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        KafkaConfig {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            request_timeout: Duration::from_secs(30),
            properties: BTreeMap::new(),
        }
    }

    pub(super) fn client_config(&self) -> Result<ClientConfig, KafkaLogError> {
        let mut config = ClientConfig::new();
        for (key, value) in &self.properties {
            let reserved = FIXED_PROPERTIES.iter().any(|(fixed, _)| fixed == key)
                || RESERVED_NAMES.contains(&key.as_str());
            if reserved {
                return Err(KafkaLogError::ReservedProperty { key: key.clone() });
            }
            config.set(key, value);
        }
        config.set("bootstrap.servers", &self.bootstrap_servers);
        for (key, value) in FIXED_PROPERTIES {
            config.set(key, value);
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_extra_properties_through() {
        let mut config = KafkaConfig::new("broker:9092", "records");
        config
            .properties
            .insert("client.id".into(), "burgess-writer-1".into());
        let client = config.client_config().unwrap();
        assert_eq!(client.get("client.id"), Some("burgess-writer-1"));
        assert_eq!(client.get("bootstrap.servers"), Some("broker:9092"));
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
        assert_eq!(client.get("auto.offset.reset"), Some("error"));
    }

    #[test]
    fn refuses_to_override_fixed_properties() {
        for key in [
            "enable.auto.commit",
            "auto.offset.reset",
            "bootstrap.servers",
            "group.id",
            "metadata.broker.list",
            "auto.commit.enable",
        ] {
            let mut config = KafkaConfig::new("broker:9092", "records");
            config.properties.insert(key.into(), "anything".into());
            assert!(
                matches!(
                    config.client_config(),
                    Err(KafkaLogError::ReservedProperty { key: refused }) if refused == key
                ),
                "{key}"
            );
        }
    }
}
