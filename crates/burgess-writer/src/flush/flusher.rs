//! The flush itself.

use std::sync::Arc;

use burgess_core::storage::{Day, DayManifest, FileStats, FlushEntry, ObjectName, WriterId};
use burgess_core::wire::UnixMillis;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use super::checkpoint::Checkpoint;
use super::clock::Clock;
use super::error::FlushError;
use crate::batch::SortedBatch;
use crate::parquet_files::{BatchFiles, FileWriter, ParquetFile};

/// Stores batches, one flush at a time, and keeps the checkpoint.
pub struct Flusher<C> {
    store: Arc<dyn ObjectStore>,
    files: Arc<FileWriter>,
    clock: C,
    writer_id: WriterId,
    checkpoint: Checkpoint,
    /// The manifest of the day last flushed to, as last stored. This writer
    /// is its only author, so it needs no reading back until the day changes.
    manifest: Option<DayManifest>,
    /// Set while a manifest may be ahead of the checkpoint; see
    /// [`FlushError::NeedsRestart`].
    needs_restart: bool,
}

impl<C: Clock> Flusher<C> {
    /// Loads the writer's checkpoint, or starts from an empty one when the
    /// writer has never flushed.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        files: FileWriter,
        clock: C,
        writer_id: WriterId,
    ) -> Result<Self, FlushError> {
        let name = ObjectName::Checkpoint {
            writer_id: writer_id.clone(),
        };
        let checkpoint = match get(store.as_ref(), &name).await? {
            Some(bytes) => Checkpoint::from_json(&bytes).map_err(FlushError::Checkpoint)?,
            None => Checkpoint::default(),
        };
        Ok(Flusher {
            store,
            files: Arc::new(files),
            clock,
            writer_id,
            checkpoint,
            manifest: None,
            needs_restart: false,
        })
    }

    /// The last stored checkpoint: where reading resumes after a restart.
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Stores `batch`: states file, refs file, day manifest, checkpoint, in
    /// that order, named by the writer clock.
    ///
    /// A failure before the manifest write leaves only unlisted files, which
    /// readers ignore, so the same batch may simply be flushed again. Once
    /// the manifest write has started, a failure may leave a listed flush the
    /// checkpoint does not know about; the flusher then refuses further
    /// flushes.
    pub async fn flush(&mut self, batch: Arc<SortedBatch>) -> Result<FlushEntry, FlushError> {
        if self.needs_restart {
            return Err(FlushError::NeedsRestart);
        }
        let nnnn = self.next_flush_time()?;
        // The day follows the flush time rather than the raw clock, so a
        // clock that went back keeps flushing into the day already in use.
        let day = Day::utc(
            nnnn.to_timestamp()
                .expect("a flush time is a valid timestamp"),
        )?;
        // Checked before any data is written: a refused flush must not leave
        // files behind, since the writer can never delete them.
        let mut manifest = self.day_manifest(day).await?;

        let files = self.encode(Arc::clone(&batch)).await?;
        let entry = FlushEntry {
            nnnn,
            seq: self.checkpoint.seq + 1,
            states_bounds: batch.states_bounds(),
            states: stats(&files.states),
            refs: stats(&files.refs),
        };
        self.put(&ObjectName::StatesFile { day, nnnn }, files.states.bytes)
            .await?;
        self.put(&ObjectName::RefsFile { day, nnnn }, files.refs.bytes)
            .await?;

        manifest.entries.push(entry.clone());
        let encoded = manifest.encode()?;
        let checkpoint = self.checkpoint.after_flush(nnnn, &batch.positions);

        self.needs_restart = true;
        self.put(&ObjectName::Manifest { day }, encoded).await?;
        let name = ObjectName::Checkpoint {
            writer_id: self.writer_id.clone(),
        };
        self.put(&name, checkpoint.to_json()).await?;
        self.checkpoint = checkpoint;
        self.manifest = Some(manifest);
        self.needs_restart = false;

        tracing::info!(
            %day,
            nnnn = u64::from(nnnn),
            seq = entry.seq,
            states_rows = entry.states.rows,
            refs_rows = entry.refs.rows,
            skipped = batch.skipped,
            "flushed batch"
        );
        Ok(entry)
    }

    /// The clock's time, but always later than the last flush.
    ///
    /// Flush times name the data files, so two flushes with the same time
    /// would overwrite listed files. A clock corrected backwards, or two
    /// flushes within one millisecond, would otherwise do exactly that.
    fn next_flush_time(&self) -> Result<UnixMillis, FlushError> {
        let now = self.clock.now();
        let millis = u64::try_from(now.as_millisecond())
            .map_err(|_| FlushError::ClockBeforeEpoch { time: now })?;
        let after_last = self
            .checkpoint
            .last_flush_nnnn
            .map_or(0, |last| u64::from(last) + 1);
        Ok(UnixMillis::from(millis.max(after_last)))
    }

    /// The manifest to append to: the cached one for the same day, else the
    /// stored one, else a new one for the day's first flush.
    async fn day_manifest(&self, day: Day) -> Result<DayManifest, FlushError> {
        let manifest = match &self.manifest {
            Some(manifest) if manifest.day == day => manifest.clone(),
            _ => match get(self.store.as_ref(), &ObjectName::Manifest { day }).await? {
                Some(bytes) => DayManifest::decode(&bytes)?,
                None => DayManifest::new(day, self.writer_id.clone()),
            },
        };
        if manifest.writer_id != self.writer_id {
            return Err(FlushError::ForeignManifest {
                day,
                owner: manifest.writer_id,
            });
        }
        if manifest.sealed {
            return Err(FlushError::SealedDay { day });
        }
        Ok(manifest)
    }

    /// Encodes the files on a blocking thread: a large batch takes seconds of
    /// CPU, which would otherwise stall reading and the batch timers.
    async fn encode(&self, batch: Arc<SortedBatch>) -> Result<BatchFiles, FlushError> {
        let files = Arc::clone(&self.files);
        match tokio::task::spawn_blocking(move || files.write_batch(&batch)).await {
            Ok(encoded) => Ok(encoded?),
            Err(failed) => std::panic::resume_unwind(failed.into_panic()),
        }
    }

    async fn put(&self, name: &ObjectName, bytes: Vec<u8>) -> Result<(), FlushError> {
        let path = Path::from(name.to_string());
        self.store.put(&path, PutPayload::from(bytes)).await?;
        Ok(())
    }
}

/// The object's bytes, or `None` if it does not exist.
async fn get(store: &dyn ObjectStore, name: &ObjectName) -> Result<Option<Bytes>, FlushError> {
    match store.get(&Path::from(name.to_string())).await {
        Ok(stored) => Ok(Some(stored.bytes().await?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn stats(file: &ParquetFile) -> FileStats {
    FileStats {
        rows: file.rows as u64,
        bytes: file.bytes.len() as u64,
    }
}
