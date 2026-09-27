//! Sealing finished days, so readers may cache their manifests for good.
//!
//! A day is finished once a later day holds a stored flush: flush times only
//! grow, so nothing more goes to the earlier day, and that flush's checkpoint
//! covers every entry of it, so a restart never changes it either.

use burgess_core::storage::{Day, DayManifest, ObjectName};

use super::clock::Clock;
use super::error::FlushError;
use super::flusher::Flusher;

impl<C: Clock> Flusher<C> {
    /// Seals the `unsealed` days, all earlier than `current`, once `current`
    /// (the manifest the flush is about to append to) holds a stored flush;
    /// in steady running that is the second flush of the next day. Returns
    /// the days still unsealed.
    ///
    /// Runs before the flush writes anything, so a failed seal leaves no data
    /// behind and the flush can simply be retried. A day found sealed
    /// already, by an attempt that failed later on, is not written again.
    pub(super) async fn seal_earlier_days(
        &self,
        current: &DayManifest,
        mut unsealed: Vec<Day>,
    ) -> Result<Vec<Day>, FlushError> {
        if current.entries.is_empty() {
            return Ok(unsealed);
        }
        for day in &unsealed {
            if let Some(manifest) = self.stored_manifest(*day).await?
                && !manifest.sealed
            {
                let sealed = DayManifest {
                    sealed: true,
                    ..manifest
                };
                self.put(&ObjectName::Manifest { day: *day }, sealed.encode()?)
                    .await?;
                tracing::info!(%day, flushes = sealed.entries.len(), "sealed day");
            }
        }
        unsealed.clear();
        Ok(unsealed)
    }
}
