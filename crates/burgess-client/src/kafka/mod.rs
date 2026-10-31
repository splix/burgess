//! Delivery of records to Kafka.
//!
//! Built on librdkafka's own queue, so `send` only enqueues and returns,
//! while a background thread batches, compresses and delivers. When Kafka is
//! slow or down the queue fills and further records are dropped with a log
//! line; the application never waits for Kafka.

mod compression;
mod config;
mod delivery;
mod error;
mod partitioner;

use std::time::Duration;

use burgess_core::flow::TreeId;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};

pub use compression::Compression;
pub use config::KafkaConfig;
pub use error::KafkaSinkError;

use self::delivery::DeliveryLog;
use self::partitioner::TreePartitioner;
use crate::message::Message;
use crate::sink::Sink;

/// A [`Sink`] that produces every message to one Kafka topic.
///
/// Records still queued when the sink is dropped are discarded, so flush it,
/// or the emitter that owns it, when the application shuts down.
pub struct KafkaSink {
    producer: ThreadedProducer<DeliveryLog, TreePartitioner>,
    topic: String,
}

impl KafkaSink {
    /// Creates the producer.
    ///
    /// Does not connect: the producer connects in the background and keeps
    /// retrying, so an application can start while Kafka is down. Fails only
    /// on a configuration librdkafka refuses.
    pub fn new(config: &KafkaConfig) -> Result<Self, KafkaSinkError> {
        let producer = config
            .client_config()?
            .create_with_context(DeliveryLog::default())?;
        Ok(KafkaSink {
            producer,
            topic: config.topic.clone(),
        })
    }
}

impl Sink for KafkaSink {
    fn send(&self, message: Message) {
        let record = BaseRecord::to(&self.topic)
            .key(&message.key().as_bytes()[..])
            .payload(message.value());
        if let Err((error, _)) = self.producer.send(record) {
            self.producer
                .context()
                .lost(Some(*message.key()), message.value().len(), &error);
        }
    }

    fn flush(&self, timeout: Duration) {
        if let Err(error) = self.producer.flush(timeout) {
            tracing::warn!(
                undelivered = self.producer.in_flight_count(),
                %error,
                "records were not delivered before shutdown"
            );
        }
    }
}

/// The tree a message is keyed by. Every message the sink sends is keyed by
/// its tree, so `None` means a message the sink did not build.
fn tree_of_key(key: Option<&[u8]>) -> Option<TreeId> {
    let bytes = <[u8; 12]>::try_from(key?).ok()?;
    Some(TreeId::from(bytes))
}

#[cfg(test)]
mod tests;
