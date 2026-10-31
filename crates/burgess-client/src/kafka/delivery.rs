//! Logging of the records Kafka did not take.

use burgess_core::flow::TreeId;
use jiff::Timestamp;
use rdkafka::ClientContext;
use rdkafka::error::KafkaError;
use rdkafka::message::Message as _;
use rdkafka::producer::{DeliveryResult, ProducerContext};

use super::partitioner::TreePartitioner;
use super::tree_of_key;
use crate::throttle::{Throttle, Verdict};

/// Logs every record that was handed to the producer but never reached
/// Kafka: refused when sent, for example because the producer's queue is
/// full, or given up on later, for example because the brokers stayed out of
/// reach.
///
/// It is the producer's context, so librdkafka calls it from its own thread
/// once it gives up on a record.
#[derive(Debug, Default)]
pub(super) struct DeliveryLog {
    throttle: Throttle,
}

impl DeliveryLog {
    /// Logs one lost record, named by its tree since the sink sees nothing
    /// else of it but its size.
    pub fn lost(&self, tree: Option<TreeId>, bytes: usize, error: &KafkaError) {
        let tree = tree.map(tracing::field::display);
        match self.throttle.admit(Timestamp::now()) {
            Verdict::Warn { suppressed } => tracing::warn!(
                tree,
                bytes,
                suppressed,
                "dropping a record kafka did not take: {error}"
            ),
            Verdict::Quiet => {
                tracing::debug!(tree, bytes, "dropping a record kafka did not take: {error}")
            }
        }
    }
}

impl ClientContext for DeliveryLog {}

impl ProducerContext<TreePartitioner> for DeliveryLog {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        if let Err((error, message)) = result {
            self.lost(tree_of_key(message.key()), message.payload_len(), error);
        }
    }

    fn get_custom_partitioner(&self) -> Option<&TreePartitioner> {
        Some(&TreePartitioner)
    }
}
