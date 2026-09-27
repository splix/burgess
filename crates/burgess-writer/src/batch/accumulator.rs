//! Pulls from the log until a batch is due.

use std::collections::VecDeque;
use std::num::NonZeroUsize;

use burgess_core::validation::Limits;
use tokio::time::Instant;

use super::collect::{Batch, SortedBatch};
use super::config::{BatchConfig, BatchConfigError};
use crate::source::{LogEntry, PartitionedLog};

/// How many entries to ask the log for at a time. Only affects how often the
/// log is called, not what ends up in a batch.
const PULL_SIZE: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

/// Turns the stream of log entries into batches.
///
/// Everything read from the log lives in the accumulator until it is handed
/// out in a batch. The log never returns an entry twice, so an entry held
/// anywhere else, such as in a future that gets dropped, would be lost for
/// good, and the next checkpoint would step over it.
pub struct Accumulator<L> {
    log: L,
    config: BatchConfig,
    limits: Limits,
    /// The batch being filled, if one has started.
    open: Option<OpenBatch>,
    /// Entries pulled but not yet added. They open the next batch when the
    /// current one fills up.
    carried_over: VecDeque<LogEntry>,
}

struct OpenBatch {
    batch: Batch,
    /// When the batch is due; `None` for an interval too long to represent.
    deadline: Option<Instant>,
}

impl<L: PartitionedLog> Accumulator<L> {
    /// Reads `log`, which must already be assigned, into batches bounded by
    /// `config`, validating records against `limits`.
    pub fn new(log: L, config: BatchConfig, limits: Limits) -> Result<Self, BatchConfigError> {
        config.check()?;
        Ok(Accumulator {
            log,
            config,
            limits,
            open: None,
            carried_over: VecDeque::new(),
        })
    }

    /// Collects the next batch, returning it once it has been open for
    /// [`BatchConfig::max_interval`] or has reached [`BatchConfig::max_bytes`].
    ///
    /// Cancellation safe, and a log error loses nothing either: the batch
    /// stays open, and the next call continues it with its original deadline.
    pub async fn next_batch(&mut self) -> Result<SortedBatch, L::Error> {
        loop {
            if self.add_carried_over() {
                return Ok(self.close());
            }
            let deadline = self.open.as_ref().and_then(|open| open.deadline);
            tokio::select! {
                // Checked first, so a log that always has data cannot keep a
                // batch open past its deadline.
                biased;
                () = wait_until(deadline) => return Ok(self.close()),
                pulled = self.log.pull(PULL_SIZE) => self.carried_over.extend(pulled?),
            }
        }
    }

    /// Opens a batch if none is open, then moves carried-over entries into it.
    /// Returns whether the batch is full.
    fn add_carried_over(&mut self) -> bool {
        let open = self.open.get_or_insert_with(|| OpenBatch {
            batch: Batch::default(),
            deadline: Instant::now().checked_add(self.config.max_interval),
        });
        while let Some(entry) = self.carried_over.pop_front() {
            open.batch.add(entry, &self.limits);
            if open.batch.bytes() >= self.config.max_bytes.get() {
                return true;
            }
        }
        false
    }

    fn close(&mut self) -> SortedBatch {
        self.open
            .take()
            .map(|open| open.batch)
            .unwrap_or_default()
            .into_sorted()
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use burgess_core::wire::{Body, PayloadType, Record, State, UnixMillis};

    use super::*;
    use crate::source::{MemoryLog, MemoryLogError, MemoryLogReader, PartitionId, Position, Start};

    const INTERVAL: Duration = Duration::from_secs(600);

    fn message(ms: u64) -> Vec<u8> {
        Record {
            flow_id: "0192f3a1b2c3d4e5f6a7b8c900000000".parse().unwrap(),
            time: UnixMillis::from(ms),
            body: Body::State(State {
                service: format!("s{ms}"),
                payload_type: PayloadType::Json.into(),
                payload: vec![b'x'; 100],
                ..State::default()
            }),
        }
        .encode()
    }

    fn log_with(count: u64) -> MemoryLog {
        let log = MemoryLog::new(1);
        for ms in 0..count {
            log.append(PartitionId::from(0), message(ms)).unwrap();
        }
        log
    }

    async fn reader(log: &MemoryLog) -> MemoryLogReader {
        let mut reader = log.reader();
        reader
            .assign([(PartitionId::from(0), Start::Earliest)].into())
            .await
            .unwrap();
        reader
    }

    fn config(max_interval: Duration, max_bytes: usize) -> BatchConfig {
        BatchConfig {
            max_interval,
            max_bytes: NonZeroUsize::new(max_bytes).unwrap(),
        }
    }

    async fn accumulator(log: &MemoryLog, max_bytes: usize) -> Accumulator<MemoryLogReader> {
        Accumulator::new(
            reader(log).await,
            config(INTERVAL, max_bytes),
            Limits::default(),
        )
        .unwrap()
    }

    fn services(batch: &SortedBatch) -> Vec<String> {
        batch.states.iter().map(|row| row.service.clone()).collect()
    }

    /// Fails its second pull, like a log with a passing network problem.
    struct FlakyLog {
        inner: MemoryLogReader,
        pulls: usize,
    }

    impl PartitionedLog for FlakyLog {
        type Error = MemoryLogError;

        async fn partitions(&self) -> Result<Vec<PartitionId>, MemoryLogError> {
            self.inner.partitions().await
        }

        async fn assign(
            &mut self,
            starts: BTreeMap<PartitionId, Start>,
        ) -> Result<(), MemoryLogError> {
            self.inner.assign(starts).await
        }

        async fn pull(
            &mut self,
            max: NonZeroUsize,
        ) -> Result<Vec<crate::source::LogEntry>, MemoryLogError> {
            self.pulls += 1;
            if self.pulls == 2 {
                return Err(MemoryLogError::UnknownPartition(PartitionId::from(9)));
            }
            self.inner.pull(max).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_when_the_interval_ends() {
        let log = log_with(3);
        let mut accumulator = accumulator(&log, usize::MAX).await;
        let started = Instant::now();
        let batch = accumulator.next_batch().await.unwrap();
        assert_eq!(started.elapsed(), INTERVAL);
        assert_eq!(services(&batch), ["s0", "s1", "s2"]);
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_an_empty_batch_when_nothing_arrives() {
        let log = MemoryLog::new(1);
        let mut accumulator = accumulator(&log, usize::MAX).await;
        assert_eq!(
            accumulator.next_batch().await.unwrap(),
            SortedBatch::default()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_when_max_bytes_is_reached() {
        let log = log_with(5);
        let two_messages = 2 * message(0).len();
        let mut accumulator = accumulator(&log, two_messages).await;
        let started = Instant::now();

        let first = accumulator.next_batch().await.unwrap();
        assert!(started.elapsed() < INTERVAL);
        assert_eq!(services(&first), ["s0", "s1"]);
        assert_eq!(first.positions[&PartitionId::from(0)], Position::from(2));

        let second = accumulator.next_batch().await.unwrap();
        assert_eq!(services(&second), ["s2", "s3"]);
    }

    #[tokio::test(start_paused = true)]
    async fn entries_left_over_from_a_full_batch_open_the_next() {
        let log = log_with(5);
        let two_messages = 2 * message(0).len();
        let mut accumulator = accumulator(&log, two_messages).await;
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.extend(services(&accumulator.next_batch().await.unwrap()));
        }
        assert_eq!(seen, ["s0", "s1", "s2", "s3", "s4"]);
    }

    #[tokio::test(start_paused = true)]
    async fn records_arriving_during_the_interval_join_the_batch() {
        let log = MemoryLog::new(1);
        let mut accumulator = accumulator(&log, usize::MAX).await;
        let appender = log.clone();
        tokio::spawn(async move {
            tokio::time::sleep(INTERVAL / 2).await;
            appender.append(PartitionId::from(0), message(7)).unwrap();
        });
        let batch = accumulator.next_batch().await.unwrap();
        assert_eq!(services(&batch), ["s7"]);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_call_keeps_the_batch_and_its_deadline() {
        let log = log_with(3);
        let mut accumulator = accumulator(&log, usize::MAX).await;
        let started = Instant::now();
        let cancelled = tokio::time::timeout(INTERVAL / 2, accumulator.next_batch()).await;
        assert!(cancelled.is_err());

        let batch = accumulator.next_batch().await.unwrap();
        assert_eq!(started.elapsed(), INTERVAL);
        assert_eq!(services(&batch), ["s0", "s1", "s2"]);
    }

    #[tokio::test(start_paused = true)]
    async fn log_error_keeps_the_batch() {
        let log = log_with(3);
        let flaky = FlakyLog {
            inner: reader(&log).await,
            pulls: 0,
        };
        let mut accumulator =
            Accumulator::new(flaky, config(INTERVAL, usize::MAX), Limits::default()).unwrap();
        assert!(accumulator.next_batch().await.is_err());

        let batch = accumulator.next_batch().await.unwrap();
        assert_eq!(services(&batch), ["s0", "s1", "s2"]);
    }

    #[tokio::test(start_paused = true)]
    async fn unrepresentable_interval_leaves_only_the_size_bound() {
        let log = log_with(2);
        let one_message = message(0).len();
        let mut accumulator = Accumulator::new(
            reader(&log).await,
            config(Duration::MAX, one_message),
            Limits::default(),
        )
        .unwrap();
        assert_eq!(services(&accumulator.next_batch().await.unwrap()), ["s0"]);
    }

    #[tokio::test]
    async fn rejects_zero_interval() {
        let log = MemoryLog::new(1);
        let created = Accumulator::new(
            reader(&log).await,
            config(Duration::ZERO, 1),
            Limits::default(),
        );
        assert_eq!(created.err(), Some(BatchConfigError::ZeroInterval));
    }
}
