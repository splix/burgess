//! The flush itself.

use std::sync::Arc;

use burgess_core::storage::{Day, DayManifest, FileStats, FlushEntry, ObjectName, WriterId};
use burgess_core::wire::UnixMillis;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use super::checkpoint::{Checkpoint, PendingFlush};
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
    pub(super) checkpoint: Checkpoint,
    /// The manifest of the day last flushed to, as last stored. This writer
    /// is its only author, so it needs no reading back until the day changes.
    manifest: Option<DayManifest>,
    /// Set while a manifest may be ahead of the checkpoint; see
    /// [`FlushError::NeedsRestart`].
    needs_restart: bool,
}

impl<C: Clock> Flusher<C> {
    /// Loads the writer's checkpoint and, if it names a flush that may have
    /// been listed without being checkpointed, drops that flush, so flushing
    /// continues exactly where the last stored flush ended.
    ///
    /// A writer that has never flushed starts from an empty checkpoint. This
    /// is also how a writer recovers after [`FlushError::NeedsRestart`].
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
        let mut flusher = Flusher {
            store,
            files: Arc::new(files),
            clock,
            writer_id,
            checkpoint,
            manifest: None,
            needs_restart: false,
        };
        flusher.reconcile().await?;
        Ok(flusher)
    }

    /// The last stored checkpoint: where reading resumes after a restart.
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Stores `batch`: states file, refs file, day manifest, checkpoint, in
    /// that order, named by the writer clock. Earlier days due for sealing
    /// are sealed first, and the checkpoint is also stored just before the
    /// manifest, naming the flush as pending.
    ///
    /// A failure before the manifest write leaves only unlisted files, which
    /// readers ignore, so the same batch may simply be flushed again. Once
    /// the manifest write has started, a failure may leave a listed flush the
    /// checkpoint does not cover; the flusher then refuses further flushes
    /// until it is opened again.
    pub async fn flush(&mut self, batch: Arc<SortedBatch>) -> Result<FlushEntry, FlushError> {
        if self.needs_restart {
            return Err(FlushError::NeedsRestart);
        }
        let (nnnn, day) = self.next_flush()?;
        // Checked before any data is written: a refused flush must not leave
        // files behind, since the writer can never delete them.
        let mut manifest = self.day_manifest(day).await?;
        let unsealed = self
            .seal_earlier_days(&manifest, self.unsealed_before(day)?)
            .await?;

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
        let pending = self.checkpoint.with_pending(PendingFlush { day, nnnn });
        let checkpoint = self
            .checkpoint
            .after_flush(nnnn, &batch.positions, unsealed);

        self.put_checkpoint(&pending).await?;
        self.needs_restart = true;
        let unfinished = |cause| FlushError::Unfinished(Box::new(cause));
        self.put(&ObjectName::Manifest { day }, encoded)
            .await
            .map_err(unfinished)?;
        self.put_checkpoint(&checkpoint).await.map_err(unfinished)?;
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

    /// The time and day the next flush gets.
    ///
    /// The time is the clock's, but always later than the last flush: flush
    /// times name the data files, and a clock corrected backwards, or two
    /// flushes within one millisecond, would otherwise overwrite listed
    /// files. The day follows that time rather than the raw clock, so a
    /// clock that went back keeps flushing into the day already in use.
    fn next_flush(&self) -> Result<(UnixMillis, Day), FlushError> {
        let now = self.clock.now();
        let millis = u64::try_from(now.as_millisecond())
            .map_err(|_| FlushError::ClockBeforeEpoch { time: now })?;
        let after_last = self
            .checkpoint
            .last_flush_nnnn
            .map_or(0, |last| u64::from(last) + 1);
        let nnnn = UnixMillis::from(millis.max(after_last));
        Ok((nnnn, day_of(nnnn)?))
    }

    /// The days to seal before a flush onto `day`: the checkpoint's list,
    /// plus the day of the last stored flush once flushing has moved past it.
    fn unsealed_before(&self, day: Day) -> Result<Vec<Day>, FlushError> {
        let mut unsealed = self.checkpoint.unsealed.clone();
        if let Some(last) = self.checkpoint.last_flush_nnnn {
            let last = day_of(last)?;
            if last < day && !unsealed.contains(&last) {
                unsealed.push(last);
            }
        }
        Ok(unsealed)
    }

    /// The manifest to append to: the cached one for the same day, else the
    /// stored one, else a new one for the day's first flush.
    async fn day_manifest(&self, day: Day) -> Result<DayManifest, FlushError> {
        let manifest = match &self.manifest {
            Some(manifest) if manifest.day == day => manifest.clone(),
            _ => self
                .stored_manifest(day)
                .await?
                .unwrap_or_else(|| DayManifest::new(day, self.writer_id.clone())),
        };
        if manifest.sealed {
            return Err(FlushError::SealedDay { day });
        }
        Ok(manifest)
    }

    /// The day's manifest as stored, if it exists and belongs to this writer.
    pub(super) async fn stored_manifest(
        &self,
        day: Day,
    ) -> Result<Option<DayManifest>, FlushError> {
        let Some(bytes) = get(self.store.as_ref(), &ObjectName::Manifest { day }).await? else {
            return Ok(None);
        };
        let manifest = DayManifest::decode(&bytes)?;
        if manifest.writer_id != self.writer_id {
            return Err(FlushError::ForeignManifest {
                day,
                owner: manifest.writer_id,
            });
        }
        Ok(Some(manifest))
    }

    /// Encodes the files on a blocking thread: a large batch takes seconds of
    /// CPU, which would otherwise stall reading and the batch timers.
    async fn encode(&self, batch: Arc<SortedBatch>) -> Result<BatchFiles, FlushError> {
        let files = Arc::clone(&self.files);
        match tokio::task::spawn_blocking(move || files.write_batch(&batch)).await {
            Ok(encoded) => Ok(encoded?),
            Err(failed) => match failed.try_into_panic() {
                Ok(panic) => std::panic::resume_unwind(panic),
                Err(_) => Err(FlushError::Cancelled),
            },
        }
    }

    async fn put_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), FlushError> {
        let name = ObjectName::Checkpoint {
            writer_id: self.writer_id.clone(),
        };
        self.put(&name, checkpoint.to_json()).await
    }

    pub(super) async fn put(&self, name: &ObjectName, bytes: Vec<u8>) -> Result<(), FlushError> {
        let path = Path::from(name.to_string());
        self.store.put(&path, PutPayload::from(bytes)).await?;
        Ok(())
    }
}

/// The UTC day of a flush time.
fn day_of(nnnn: UnixMillis) -> Result<Day, FlushError> {
    let time = nnnn
        .to_timestamp()
        .expect("flush times come from timestamps, so they convert back");
    Ok(Day::utc(time)?)
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
