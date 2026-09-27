//! Why the Kafka source failed.

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;

use crate::source::{PartitionId, Position};

/// Why the Kafka topic could not be read.
#[derive(Debug, thiserror::Error)]
pub enum KafkaLogError {
    /// librdkafka reported an error.
    #[error("kafka request failed")]
    Kafka(#[from] KafkaError),
    /// The configured topic does not exist.
    #[error("kafka topic {topic} does not exist")]
    UnknownTopic {
        /// The topic.
        topic: String,
    },
    /// The topic exists but cannot be read now, for example while its
    /// leader is elected or because the writer lacks permission.
    #[error("kafka topic {topic} is unavailable: {code}")]
    TopicUnavailable {
        /// The topic.
        topic: String,
        /// What the broker reported.
        code: RDKafkaErrorCode,
    },
    /// The topic has no such partition.
    #[error("kafka topic has no partition {0}")]
    UnknownPartition(PartitionId),
    /// A position past the end of its partition, so it was not handed out by
    /// this topic; most likely a checkpoint from another topic or cluster.
    #[error("position {position} is past the end ({end}) of partition {partition}")]
    PositionOutOfRange {
        /// The partition.
        partition: PartitionId,
        /// The position asked for.
        position: Position,
        /// The partition's end: where the next record will go.
        end: u64,
    },
    /// A blocking request was cancelled, because the runtime is shutting
    /// down.
    #[error("kafka request cancelled")]
    Cancelled,
    /// A configured property would override one the source sets itself.
    #[error("kafka property {key} is set by the writer and cannot be configured")]
    ReservedProperty {
        /// The property.
        key: String,
    },
}
