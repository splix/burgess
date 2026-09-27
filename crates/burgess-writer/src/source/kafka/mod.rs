//! The Kafka topic as a partitioned log.
//!
//! Every partition is assigned by hand and positions are never committed to
//! Kafka: the writer's checkpoint is the only record of how far it got, so a
//! restart reads exactly what the checkpoint says, whatever Kafka remembers.

mod config;
mod consumer_error;
mod error;

pub use config::KafkaConfig;
pub use error::KafkaLogError;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{Offset, TopicPartitionList};

use self::consumer_error::ConsumerError;
use super::{LogEntry, PartitionId, PartitionedLog, Position, Start};

/// Reads the configured Kafka topic.
pub struct KafkaLog {
    consumer: Arc<StreamConsumer>,
    topic: String,
    request_timeout: Duration,
    /// Where each assigned partition continues: its start, then the position
    /// after the last entry handed out. Lets the log reassign itself exactly
    /// when Kafka loses a position.
    positions: BTreeMap<PartitionId, Start>,
    /// Kafka reported a position it no longer holds. The next pull reassigns
    /// every partition first; kept as a flag so a pull cancelled mid-way
    /// still does it.
    reset_seen: bool,
    /// A fatal error met after some entries were already collected. Those
    /// entries are returned first and the error with the next pull, so
    /// nothing read is dropped.
    deferred: Option<KafkaError>,
}

impl KafkaLog {
    /// Creates the consumer. librdkafka starts connecting to the brokers in
    /// the background right away; nothing is assigned yet.
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaLogError> {
        let consumer = config.client_config()?.create()?;
        Ok(KafkaLog {
            consumer: Arc::new(consumer),
            topic: config.topic,
            request_timeout: config.request_timeout,
            positions: BTreeMap::new(),
            reset_seen: false,
            deferred: None,
        })
    }

    /// Runs one of librdkafka's blocking requests on a blocking thread.
    async fn blocking<T: Send + 'static>(
        &self,
        request: impl FnOnce(&StreamConsumer) -> Result<T, KafkaLogError> + Send + 'static,
    ) -> Result<T, KafkaLogError> {
        let consumer = Arc::clone(&self.consumer);
        match tokio::task::spawn_blocking(move || request(&consumer)).await {
            Ok(result) => result,
            Err(failed) => match failed.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(_) => Err(KafkaLogError::Cancelled),
            },
        }
    }

    /// The offsets to start the given partitions at, for their stored
    /// positions, fetched in one blocking task.
    ///
    /// A position before the oldest record Kafka still holds means the
    /// writer was away longer than the topic's retention. Those records are
    /// gone for good, so reading continues from the oldest one left, loudly.
    /// A position past the end was never handed out by this topic.
    async fn start_offsets(
        &self,
        positions: Vec<(PartitionId, Position)>,
    ) -> Result<Vec<(PartitionId, Offset)>, KafkaLogError> {
        let topic = self.topic.clone();
        let timeout = self.request_timeout;
        self.blocking(move |consumer| {
            positions
                .into_iter()
                .map(|(partition, position)| {
                    let (earliest, end) =
                        consumer.fetch_watermarks(&topic, kafka_partition(partition), timeout)?;
                    Ok((partition, start_offset(partition, position, earliest, end)?))
                })
                .collect()
        })
        .await
    }

    /// Reassigns every partition to where it had got to, after Kafka lost a
    /// position; the partition that lost it resumes at its oldest record.
    async fn reassign(&mut self) -> Result<(), KafkaLogError> {
        self.reset_seen = true;
        self.assign(self.positions.clone()).await?;
        self.reset_seen = false;
        Ok(())
    }

    /// Hands out a message as an entry and remembers where its partition
    /// continues.
    fn take(&mut self, message: &BorrowedMessage<'_>) -> LogEntry {
        let entry = entry(message);
        self.positions
            .insert(entry.partition, Start::At(entry.resume_at));
        entry
    }

    /// Acts on an error from the consumer: temporary ones are logged and
    /// waited out, since librdkafka retries them itself; a lost position
    /// marks the log for reassignment; anything else is returned.
    fn handle(&mut self, error: KafkaError) {
        match ConsumerError::from(error) {
            ConsumerError::Temporary(code) => {
                tracing::warn!(%code, "kafka consumer error; librdkafka retries by itself");
            }
            ConsumerError::PositionLost => {
                tracing::error!(
                    topic = %self.topic,
                    "kafka no longer holds a position being read; reassigning"
                );
                self.reset_seen = true;
            }
            ConsumerError::Fatal(error) => self.deferred = Some(error),
        }
    }
}

impl PartitionedLog for KafkaLog {
    type Error = KafkaLogError;

    async fn partitions(&self) -> Result<Vec<PartitionId>, KafkaLogError> {
        let topic = self.topic.clone();
        let timeout = self.request_timeout;
        self.blocking(move |consumer| {
            let metadata = consumer.fetch_metadata(Some(&topic), timeout)?;
            let Some(listed) = metadata
                .topics()
                .iter()
                .find(|listed| listed.name() == topic)
            else {
                return Err(KafkaLogError::UnknownTopic { topic });
            };
            if let Some(error) = listed.error() {
                let code = RDKafkaErrorCode::from(error);
                if code == RDKafkaErrorCode::UnknownTopicOrPartition {
                    return Err(KafkaLogError::UnknownTopic { topic });
                }
                return Err(KafkaLogError::TopicUnavailable { topic, code });
            }
            let mut partitions: Vec<PartitionId> = listed
                .partitions()
                .iter()
                .map(|partition| PartitionId::from(partition.id() as u32))
                .collect();
            partitions.sort();
            Ok(partitions)
        })
        .await
    }

    async fn assign(&mut self, starts: BTreeMap<PartitionId, Start>) -> Result<(), KafkaLogError> {
        let known = self.partitions().await?;
        if let Some(unknown) = starts.keys().find(|partition| !known.contains(partition)) {
            return Err(KafkaLogError::UnknownPartition(*unknown));
        }
        let stored: Vec<(PartitionId, Position)> = starts
            .iter()
            .filter_map(|(partition, start)| match start {
                Start::At(position) => Some((*partition, *position)),
                Start::Earliest => None,
            })
            .collect();
        let mut offsets: BTreeMap<PartitionId, Offset> =
            self.start_offsets(stored).await?.into_iter().collect();

        let mut assignment = TopicPartitionList::new();
        for partition in starts.keys() {
            let offset = offsets.remove(partition).unwrap_or(Offset::Beginning);
            assignment.add_partition_offset(&self.topic, kafka_partition(*partition), offset)?;
        }
        self.blocking(move |consumer| Ok(consumer.assign(&assignment)?))
            .await?;
        self.positions = starts;
        self.deferred = None;
        Ok(())
    }

    async fn pull(&mut self, max: NonZeroUsize) -> Result<Vec<LogEntry>, KafkaLogError> {
        let consumer = Arc::clone(&self.consumer);
        loop {
            if let Some(error) = self.deferred.take() {
                return Err(error.into());
            }
            if self.reset_seen {
                self.reassign().await?;
            }
            let first = match consumer.recv().await {
                Ok(message) => message,
                Err(error) => {
                    self.handle(error);
                    continue;
                }
            };
            let mut entries = vec![self.take(&first)];
            // Take what is already buffered without waiting again; `recv` is
            // cancellation safe, and nothing here awaits, so no entry can be
            // lost if the caller drops this future.
            while entries.len() < max.get() {
                match consumer.recv().now_or_never() {
                    Some(Ok(message)) => entries.push(self.take(&message)),
                    Some(Err(error)) => {
                        self.handle(error);
                        break;
                    }
                    None => break,
                }
            }
            return Ok(entries);
        }
    }
}

/// Where to start a partition for a stored position, given the oldest
/// offset Kafka holds and its end; see [`KafkaLog::start_offsets`].
fn start_offset(
    partition: PartitionId,
    position: Position,
    earliest: i64,
    end: i64,
) -> Result<Offset, KafkaLogError> {
    let requested = i64::try_from(u64::from(position)).unwrap_or(i64::MAX);
    if requested > end {
        return Err(KafkaLogError::PositionOutOfRange {
            partition,
            position,
            end: end as u64,
        });
    }
    if requested < earliest {
        tracing::error!(
            %partition,
            %position,
            earliest,
            lost = earliest - requested,
            "records before the earliest offset Kafka holds are lost; resuming there"
        );
        return Ok(Offset::Offset(earliest));
    }
    Ok(Offset::Offset(requested))
}

/// A message without a payload becomes an empty one, which the batch then
/// skips as malformed like any other unreadable message.
fn entry(message: &BorrowedMessage<'_>) -> LogEntry {
    LogEntry {
        partition: PartitionId::from(message.partition() as u32),
        payload: message.payload().unwrap_or_default().to_vec(),
        resume_at: Position::from(message.offset() as u64 + 1),
    }
}

/// Kafka numbers partitions with an `i32`; ids come from its metadata, so
/// they always fit.
fn kafka_partition(partition: PartitionId) -> i32 {
    i32::try_from(u32::from(partition)).expect("partition ids come from Kafka and fit an i32")
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod start_offset_tests {
    use super::*;

    fn offset(position: u64, earliest: i64, end: i64) -> Result<Offset, KafkaLogError> {
        start_offset(
            PartitionId::from(0),
            Position::from(position),
            earliest,
            end,
        )
    }

    #[test]
    fn stored_position_within_the_partition_is_kept() {
        assert!(matches!(offset(5, 2, 9), Ok(Offset::Offset(5))));
        assert!(matches!(offset(9, 2, 9), Ok(Offset::Offset(9))));
    }

    #[test]
    fn position_before_the_retained_records_resumes_at_the_earliest() {
        assert!(matches!(offset(1, 4, 9), Ok(Offset::Offset(4))));
    }

    #[test]
    fn position_past_the_end_is_an_error() {
        assert!(matches!(
            offset(10, 2, 9),
            Err(KafkaLogError::PositionOutOfRange { end: 9, .. })
        ));
    }
}
