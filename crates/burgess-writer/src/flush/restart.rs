//! Bringing the manifest back in line with the checkpoint on start.
//!
//! A flush that died after its manifest write but before its final
//! checkpoint left a listed flush whose records the checkpoint says were
//! never stored. The writer is about to read and store those records again,
//! so the listed copy must go first, or readers would see every row of it
//! twice. The checkpoint names the one day this can have happened to.

use burgess_core::storage::ObjectName;

use super::checkpoint::PendingFlush;
use super::clock::Clock;
use super::error::FlushError;
use super::flusher::Flusher;

impl<C: Clock> Flusher<C> {
    /// Drops the pending flush, if the checkpoint names one, from its day's
    /// manifest. Without a pending flush there is nothing to fix: every
    /// manifest write is preceded by a checkpoint that names it.
    pub(super) async fn reconcile(&mut self) -> Result<(), FlushError> {
        if let Some(pending) = self.checkpoint.pending {
            self.drop_uncovered(pending).await?;
            self.checkpoint.pending = None;
        }
        Ok(())
    }

    async fn drop_uncovered(&self, pending: PendingFlush) -> Result<(), FlushError> {
        let day = pending.day;
        let Some(mut manifest) = self.stored_manifest(day).await? else {
            return Ok(());
        };
        let listed = manifest.entries.len();
        manifest
            .entries
            .retain(|entry| entry.seq <= self.checkpoint.seq);
        let dropped = listed - manifest.entries.len();
        if dropped == 0 {
            return Ok(());
        }
        // Sealing waits until the checkpoint covers the whole day, so a
        // sealed day with uncovered entries means the checkpoint or the
        // manifest is not the one this writer wrote. Readers may have cached
        // the sealed manifest for good, so it must not change either way.
        if manifest.sealed {
            return Err(FlushError::SealedManifestAhead { day });
        }
        self.put(&ObjectName::Manifest { day }, manifest.encode()?)
            .await?;
        tracing::warn!(
            %day,
            dropped,
            checkpoint_seq = self.checkpoint.seq,
            "dropped manifest entries of a flush that never reached its checkpoint"
        );
        Ok(())
    }
}
