//! The writer's main loop: collect a batch, store it, repeat, stop cleanly.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::batch::{Accumulator, SortedBatch};
use crate::flush::{Clock, FlushError, Flusher};
use crate::source::PartitionedLog;

/// First wait before storing a batch again after the object store failed.
const FIRST_RETRY: Duration = Duration::from_secs(1);

/// Longest wait between attempts; an outage longer than this is retried at
/// this pace until it ends or the writer is stopped.
const LONGEST_RETRY: Duration = Duration::from_secs(60);

/// Why the writer stopped.
#[derive(Debug, thiserror::Error)]
pub enum PipelineError<E: std::error::Error + 'static> {
    /// The log failed in a way it cannot recover from.
    #[error("reading the log failed")]
    Log(#[source] E),
    /// A batch could not be stored. For [`FlushError::Unfinished`] a restart
    /// recovers; other errors need an operator.
    #[error("storing a batch failed")]
    Flush(#[from] FlushError),
}

/// Moves batches from the log to storage until told to stop.
pub struct Pipeline<L, C> {
    accumulator: Accumulator<L>,
    flusher: Flusher<C>,
}

impl<L: PartitionedLog, C: Clock> Pipeline<L, C> {
    /// A pipeline over a log already resumed from the flusher's checkpoint.
    pub fn new(accumulator: Accumulator<L>, flusher: Flusher<C>) -> Self {
        Pipeline {
            accumulator,
            flusher,
        }
    }

    /// Runs until `stop` turns true, then stores what was read since the
    /// last flush, if anything, and returns.
    ///
    /// A flush already running when the stop comes is finished first: cut
    /// short, it could leave a listed flush the checkpoint does not know
    /// about.
    pub async fn run(
        mut self,
        mut stop: watch::Receiver<bool>,
    ) -> Result<(), PipelineError<L::Error>> {
        loop {
            let batch = tokio::select! {
                biased;
                _ = stop.wait_for(|stop| *stop) => break,
                batch = self.accumulator.next_batch() => batch.map_err(PipelineError::Log)?,
            };
            self.store(Arc::new(batch), &mut stop).await?;
        }
        if let Some(batch) = self.accumulator.finish() {
            tracing::info!("stopping: storing the batch read so far");
            self.store(Arc::new(batch), &mut stop).await?;
        }
        Ok(())
    }

    /// Stores `batch`, trying again while the object store fails.
    ///
    /// Only plain store failures are retried: once a retry could store the
    /// batch twice the flusher reports [`FlushError::Unfinished`] instead,
    /// which ends the loop like any other error.
    /// Once the writer is stopping it stops retrying, and the batch is read
    /// again after the restart.
    async fn store(
        &mut self,
        batch: Arc<SortedBatch>,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<(), FlushError> {
        let mut wait = FIRST_RETRY;
        loop {
            let error = match self.flusher.flush(Arc::clone(&batch)).await {
                Ok(_) => return Ok(()),
                Err(error @ FlushError::Store(_)) => error,
                Err(error) => return Err(error),
            };
            if *stop.borrow() {
                return Err(error);
            }
            tracing::warn!(
                error = %crate::error_chain(&error),
                retry_in = ?wait,
                "storing a batch failed; trying again"
            );
            tokio::select! {
                _ = stop.wait_for(|stop| *stop) => return Err(error),
                () = tokio::time::sleep(wait) => {}
            }
            wait = (wait * 2).min(LONGEST_RETRY);
        }
    }
}

#[cfg(test)]
mod tests;
