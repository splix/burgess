//! A partitioned log held in memory, for tests.
//!
//! It keeps writer tests fast and independent of a running Kafka while
//! behaving the same way where the writer can tell: entries keep their order
//! within a partition, positions resume exactly after an entry, a bad position
//! is refused, and a pull waits for new entries instead of returning nothing.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

use super::PartitionedLog;
use super::entry::LogEntry;
use super::position::{PartitionId, Position, Start};

/// Why the in-memory log refused a request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemoryLogError {
    /// The log has no such partition.
    #[error("unknown partition {0}")]
    UnknownPartition(PartitionId),
    /// The position is beyond the end of the partition, so it was not handed
    /// out by this log. Resuming there would skip every entry before it.
    #[error("position {position} is beyond the end of partition {partition}")]
    PositionOutOfRange {
        /// The partition.
        partition: PartitionId,
        /// The position asked for.
        position: Position,
    },
}

/// The contents of an in-memory log, shared by everyone appending and reading.
///
/// Cloning gives another handle to the same log, so a test can keep appending
/// while a reader is in use, and can open a fresh reader to simulate a writer
/// restart.
#[derive(Debug, Clone)]
pub struct MemoryLog {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// Messages of each partition, indexed by partition id.
    partitions: Mutex<Vec<Vec<Vec<u8>>>>,
    appended: Notify,
}

impl MemoryLog {
    /// Creates an empty log with partitions `0..partition_count`.
    pub fn new(partition_count: u32) -> Self {
        MemoryLog {
            shared: Arc::new(Shared {
                partitions: Mutex::new(vec![Vec::new(); partition_count as usize]),
                appended: Notify::new(),
            }),
        }
    }

    /// Adds a message to the end of `partition`.
    pub fn append(&self, partition: PartitionId, payload: Vec<u8>) -> Result<(), MemoryLogError> {
        self.shared
            .lock()
            .get_mut(index(partition))
            .ok_or(MemoryLogError::UnknownPartition(partition))?
            .push(payload);
        self.shared.appended.notify_waiters();
        Ok(())
    }

    /// Opens a reader with nothing assigned yet.
    pub fn reader(&self) -> MemoryLogReader {
        MemoryLogReader {
            log: self.clone(),
            cursors: Vec::new(),
            next_turn: 0,
        }
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Vec<Vec<Vec<u8>>>> {
        // A panic while holding the lock cannot leave the vectors half
        // updated, so a poisoned lock is still safe to use.
        self.partitions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Reads a [`MemoryLog`], remembering how far it got in each partition.
#[derive(Debug)]
pub struct MemoryLogReader {
    log: MemoryLog,
    /// Assigned partitions, in id order.
    cursors: Vec<Cursor>,
    /// Index into `cursors` of the partition whose turn comes next.
    next_turn: usize,
}

#[derive(Debug)]
struct Cursor {
    partition: PartitionId,
    /// Index of the next message to read.
    next: usize,
}

impl MemoryLogReader {
    /// Takes up to `max` available entries, one partition at a time in turn.
    ///
    /// The turn carries over from one pull to the next, so a busy partition
    /// cannot hide the others even when `max` is smaller than the number of
    /// partitions.
    fn take(&mut self, max: usize) -> Vec<LogEntry> {
        let partitions = self.log.shared.lock();
        let mut entries = Vec::new();
        let mut idle_in_a_row = 0;
        while entries.len() < max && idle_in_a_row < self.cursors.len() {
            let turn = self.next_turn % self.cursors.len();
            self.next_turn = turn + 1;
            let cursor = &mut self.cursors[turn];
            match partitions[index(cursor.partition)].get(cursor.next) {
                Some(payload) => {
                    cursor.next += 1;
                    entries.push(LogEntry {
                        partition: cursor.partition,
                        payload: payload.clone(),
                        resume_at: Position::from(cursor.next as u64),
                    });
                    idle_in_a_row = 0;
                }
                None => idle_in_a_row += 1,
            }
        }
        entries
    }
}

impl PartitionedLog for MemoryLogReader {
    type Error = MemoryLogError;

    async fn partitions(&self) -> Result<Vec<PartitionId>, MemoryLogError> {
        let count = self.log.shared.lock().len() as u32;
        Ok((0..count).map(PartitionId::from).collect())
    }

    async fn assign(&mut self, starts: BTreeMap<PartitionId, Start>) -> Result<(), MemoryLogError> {
        let partitions = self.log.shared.lock();
        let cursors = starts
            .into_iter()
            .map(|(partition, start)| {
                let entries = partitions
                    .get(index(partition))
                    .ok_or(MemoryLogError::UnknownPartition(partition))?;
                // Nothing is ever dropped from a memory log, so the earliest
                // entry is always the first one.
                let next = match start {
                    Start::Earliest => 0,
                    Start::At(position) => usize::try_from(u64::from(position))
                        .ok()
                        .filter(|next| *next <= entries.len())
                        .ok_or(MemoryLogError::PositionOutOfRange {
                            partition,
                            position,
                        })?,
                };
                Ok(Cursor { partition, next })
            })
            .collect::<Result<_, _>>()?;
        drop(partitions);
        self.cursors = cursors;
        self.next_turn = 0;
        Ok(())
    }

    async fn pull(&mut self, max: NonZeroUsize) -> Result<Vec<LogEntry>, MemoryLogError> {
        let shared = Arc::clone(&self.log.shared);
        loop {
            // Registered before looking, so an append that lands between the
            // look and the wait still wakes this pull.
            let appended = shared.appended.notified();
            let entries = self.take(max.get());
            if !entries.is_empty() {
                return Ok(entries);
            }
            appended.await;
        }
    }
}

fn index(partition: PartitionId) -> usize {
    u32::from(partition) as usize
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::source::ResumeError;

    fn partition(id: u32) -> PartitionId {
        PartitionId::from(id)
    }

    fn up_to(max: usize) -> NonZeroUsize {
        NonZeroUsize::new(max).unwrap()
    }

    fn payloads(entries: &[LogEntry]) -> Vec<&[u8]> {
        entries
            .iter()
            .map(|entry| entry.payload.as_slice())
            .collect()
    }

    fn log_with(partitions: &[&[&str]]) -> MemoryLog {
        let log = MemoryLog::new(partitions.len() as u32);
        for (id, messages) in partitions.iter().enumerate() {
            for message in *messages {
                log.append(partition(id as u32), message.as_bytes().to_vec())
                    .unwrap();
            }
        }
        log
    }

    /// Two partitions: `a0..a4` in partition 0, `b0..b2` in partition 1.
    fn filled_log() -> MemoryLog {
        log_with(&[&["a0", "a1", "a2", "a3", "a4"], &["b0", "b1", "b2"]])
    }

    async fn reader_from(
        log: &MemoryLog,
        starts: impl IntoIterator<Item = (u32, Start)>,
    ) -> MemoryLogReader {
        let mut reader = log.reader();
        reader
            .assign(
                starts
                    .into_iter()
                    .map(|(id, start)| (partition(id), start))
                    .collect(),
            )
            .await
            .unwrap();
        reader
    }

    fn from_earliest(ids: impl IntoIterator<Item = u32>) -> Vec<(u32, Start)> {
        ids.into_iter().map(|id| (id, Start::Earliest)).collect()
    }

    #[tokio::test]
    async fn lists_all_partitions() {
        let partitions = MemoryLog::new(3).reader().partitions().await.unwrap();
        assert_eq!(partitions, [partition(0), partition(1), partition(2)]);
    }

    #[tokio::test]
    async fn reads_each_partition_in_order_taking_turns() {
        let mut reader = reader_from(&filled_log(), from_earliest(0..2)).await;
        let entries = reader.pull(up_to(100)).await.unwrap();
        assert_eq!(
            payloads(&entries),
            [b"a0", b"b0", b"a1", b"b1", b"a2", b"b2", b"a3", b"a4"]
        );
    }

    #[tokio::test]
    async fn turns_carry_over_between_small_pulls() {
        let log = log_with(&[&["a0", "a1"], &["b0", "b1"], &["c0", "c1"]]);
        let mut reader = reader_from(&log, from_earliest(0..3)).await;
        assert_eq!(
            payloads(&reader.pull(up_to(2)).await.unwrap()),
            [b"a0", b"b0"]
        );
        assert_eq!(
            payloads(&reader.pull(up_to(2)).await.unwrap()),
            [b"c0", b"a1"]
        );
        assert_eq!(
            payloads(&reader.pull(up_to(2)).await.unwrap()),
            [b"b1", b"c1"]
        );
    }

    #[tokio::test]
    async fn pull_returns_at_most_max() {
        let mut reader = reader_from(&filled_log(), from_earliest(0..2)).await;
        assert_eq!(reader.pull(up_to(3)).await.unwrap().len(), 3);
        assert_eq!(reader.pull(up_to(100)).await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn resume_returns_exactly_the_entries_after_the_position() {
        let log = filled_log();
        let mut first = reader_from(&log, from_earliest(0..2)).await;
        let seen = first.pull(up_to(4)).await.unwrap();
        assert_eq!(payloads(&seen), [b"a0", b"b0", b"a1", b"b1"]);

        let starts = [
            (0, Start::At(seen[2].resume_at)),
            (1, Start::At(seen[3].resume_at)),
        ];
        let mut restarted = reader_from(&log, starts).await;
        let rest = restarted.pull(up_to(100)).await.unwrap();
        assert_eq!(payloads(&rest), [b"a2", b"b2", b"a3", b"a4"]);
    }

    #[tokio::test]
    async fn resume_position_of_an_entry_is_stable_across_readers() {
        let log = filled_log();
        let mut one = reader_from(&log, from_earliest(0..2)).await;
        let mut two = reader_from(&log, from_earliest(0..2)).await;
        assert_eq!(
            one.pull(up_to(100)).await.unwrap(),
            two.pull(up_to(100)).await.unwrap()
        );
    }

    #[tokio::test]
    async fn unassigned_partitions_are_not_read() {
        let mut reader = reader_from(&filled_log(), from_earliest([1])).await;
        assert_eq!(
            payloads(&reader.pull(up_to(100)).await.unwrap()),
            [b"b0", b"b1", b"b2"]
        );
    }

    #[tokio::test]
    async fn pull_waits_for_an_append() {
        let log = MemoryLog::new(1);
        let mut reader = reader_from(&log, from_earliest([0])).await;
        let appender = log.clone();
        let append = tokio::spawn(async move {
            tokio::task::yield_now().await;
            appender.append(partition(0), b"late".to_vec()).unwrap();
        });
        let entries = reader.pull(up_to(10)).await.unwrap();
        append.await.unwrap();
        assert_eq!(payloads(&entries), [b"late"]);
    }

    #[tokio::test]
    async fn cancelled_pull_loses_nothing() {
        let log = MemoryLog::new(1);
        let mut reader = reader_from(&log, from_earliest([0])).await;
        let timed_out =
            tokio::time::timeout(Duration::from_millis(10), reader.pull(up_to(10))).await;
        assert!(timed_out.is_err());
        log.append(partition(0), b"after".to_vec()).unwrap();
        assert_eq!(payloads(&reader.pull(up_to(10)).await.unwrap()), [b"after"]);
    }

    #[tokio::test]
    async fn first_run_resumes_every_partition_from_the_start() {
        let mut reader = filled_log().reader();
        reader.resume(&BTreeMap::new()).await.unwrap();
        assert_eq!(reader.pull(up_to(100)).await.unwrap().len(), 8);
    }

    #[tokio::test]
    async fn resume_continues_known_partitions_and_starts_new_ones() {
        let log = filled_log();
        let mut first = reader_from(&log, from_earliest([0])).await;
        let seen = first.pull(up_to(2)).await.unwrap();
        assert_eq!(payloads(&seen), [b"a0", b"a1"]);

        // Partition 1 was never read, as if it had been added since.
        let known = BTreeMap::from([(partition(0), seen[1].resume_at)]);
        let mut restarted = log.reader();
        restarted.resume(&known).await.unwrap();
        assert_eq!(
            payloads(&restarted.pull(up_to(100)).await.unwrap()),
            [b"a2", b"b0", b"a3", b"b1", b"a4", b"b2"]
        );
    }

    #[tokio::test]
    async fn resume_refuses_partitions_the_log_does_not_have() {
        let log = MemoryLog::new(2);
        let known = BTreeMap::from([
            (partition(1), Position::from(0)),
            (partition(3), Position::from(4)),
            (partition(7), Position::from(9)),
        ]);
        let error = log.reader().resume(&known).await.unwrap_err();
        assert!(
            matches!(&error, ResumeError::UnknownPartitions(unknown) if *unknown == [partition(3), partition(7)]),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "the log has no partitions 3, 7, which the checkpoint has positions for"
        );
    }

    #[tokio::test]
    async fn resume_passes_on_log_errors() {
        let log = filled_log();
        let known = BTreeMap::from([(partition(1), Position::from(99))]);
        let error = log.reader().resume(&known).await.unwrap_err();
        assert!(
            matches!(
                error,
                ResumeError::Log(MemoryLogError::PositionOutOfRange { .. })
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn position_at_the_end_waits_for_new_entries() {
        let log = filled_log();
        let mut reader = reader_from(&log, [(1, Start::At(Position::from(3)))]).await;
        log.append(partition(1), b"b3".to_vec()).unwrap();
        assert_eq!(payloads(&reader.pull(up_to(10)).await.unwrap()), [b"b3"]);
    }

    #[tokio::test]
    async fn rejects_position_beyond_the_end() {
        let mut reader = filled_log().reader();
        let assigned = reader
            .assign([(partition(1), Start::At(Position::from(4)))].into())
            .await;
        assert_eq!(
            assigned,
            Err(MemoryLogError::PositionOutOfRange {
                partition: partition(1),
                position: Position::from(4)
            })
        );
    }

    #[tokio::test]
    async fn rejects_unknown_partitions() {
        let log = MemoryLog::new(1);
        assert_eq!(
            log.append(partition(1), Vec::new()),
            Err(MemoryLogError::UnknownPartition(partition(1)))
        );
        let assigned = log
            .reader()
            .assign([(partition(5), Start::Earliest)].into())
            .await;
        assert_eq!(
            assigned,
            Err(MemoryLogError::UnknownPartition(partition(5)))
        );
    }
}
