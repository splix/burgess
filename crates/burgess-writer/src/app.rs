//! Starting the writer from its settings.

use tokio::sync::watch;

use crate::batch::{Accumulator, BatchConfigError};
use crate::config::Settings;
use crate::flush::{FlushError, Flusher, SystemClock};
use crate::parquet_files::{FileError, FileWriter};
use crate::pipeline::{Pipeline, PipelineError};
use crate::source::{KafkaLog, KafkaLogError, PartitionedLog, ResumeError};

/// Why the writer could not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// The object store could not be set up.
    #[error("opening the object store failed")]
    Store(#[from] object_store::Error),
    /// The Parquet settings are invalid.
    #[error("parquet settings are invalid")]
    Files(#[from] FileError),
    /// The Kafka consumer could not be created.
    #[error("creating the kafka consumer failed")]
    Kafka(#[from] KafkaLogError),
    /// Reading could not continue from the checkpoint.
    #[error("resuming from the checkpoint failed")]
    Resume(#[from] ResumeError<KafkaLogError>),
    /// The batch settings are invalid.
    #[error(transparent)]
    Batch(#[from] BatchConfigError),
    /// The checkpoint could not be loaded or reconciled.
    #[error("opening the flusher failed")]
    Flusher(#[from] FlushError),
    /// The writer stopped with an error while running.
    #[error("the writer stopped")]
    Pipeline(#[from] PipelineError<KafkaLogError>),
}

/// Runs the writer until `stop` turns true.
///
/// The flusher opens first, so that reading resumes from the checkpoint as
/// it stands after reconciling.
pub async fn run(settings: Settings, stop: watch::Receiver<bool>) -> Result<(), AppError> {
    tracing::info!(
        writer_id = %settings.writer_id,
        topic = %settings.kafka.topic,
        batch_max_interval = ?settings.batch.max_interval,
        batch_max_bytes = settings.batch.max_bytes,
        max_payload_bytes = settings.limits.max_payload_bytes,
        zstd_level = settings.parquet.zstd_level,
        bloom_target_fpp = f64::from(settings.bloom.target_fpp),
        bloom_min_bytes = settings.bloom.min_bytes,
        "starting"
    );
    let store = settings.store.open()?;
    let files = FileWriter::new(settings.parquet)?;
    let flusher = Flusher::open(
        store,
        files,
        SystemClock,
        settings.writer_id,
        settings.bloom,
    )
    .await?;

    let mut log = KafkaLog::new(settings.kafka)?;
    log.resume(&flusher.checkpoint().positions).await?;
    let accumulator = Accumulator::new(log, settings.batch, settings.limits)?;

    Pipeline::new(accumulator, flusher).run(stop).await?;
    tracing::info!("stopped");
    Ok(())
}
