//! Why a Kafka sink could not be created.

use rdkafka::error::KafkaError;

/// Why a [`KafkaSink`](super::KafkaSink) could not be created.
///
/// Only creation can fail. Once running, the sink logs failed sends instead
/// of returning them, so the application never has to handle them.
#[derive(Debug, thiserror::Error)]
pub enum KafkaSinkError {
    /// librdkafka refused the configuration.
    #[error("kafka producer cannot be created")]
    Kafka(#[from] KafkaError),
    /// A configured property would override one the sink sets itself.
    #[error("kafka property {key} is set by the sink and cannot be configured")]
    ReservedProperty {
        /// The property.
        key: String,
    },
}
