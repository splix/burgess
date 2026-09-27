//! Why a flush failed.

use burgess_core::storage::{Day, DayError, ManifestError, WriterId};
use jiff::Timestamp;

use crate::parquet_files::FileError;

/// Why a batch could not be stored.
#[derive(Debug, thiserror::Error)]
pub enum FlushError {
    /// Encoding the Parquet files failed.
    #[error("writing parquet files failed")]
    Files(#[from] FileError),
    /// The object store refused a read or a write.
    #[error("object store request failed")]
    Store(#[from] object_store::Error),
    /// The stored manifest is unreadable, or the new one could not be built.
    #[error("day manifest failed")]
    Manifest(#[from] ManifestError),
    /// The stored checkpoint is not valid JSON of the expected shape.
    /// Starting over instead would read every retained record again.
    #[error("stored checkpoint is unreadable")]
    Checkpoint(#[source] serde_json::Error),
    /// The writer clock is before 1970, where flush times cannot name files.
    #[error("writer clock reads {time}, before 1970")]
    ClockBeforeEpoch {
        /// What the clock read.
        time: Timestamp,
    },
    /// The flush time gives a day that cannot be stored.
    #[error("flush time is out of range")]
    Day(#[from] DayError),
    /// The day is sealed, so its manifest must never change again; the
    /// writer clock has most likely gone back a day or more.
    #[error("day {day} is sealed and cannot take new flushes")]
    SealedDay {
        /// The sealed day.
        day: Day,
    },
    /// The day's manifest belongs to another writer. Appending to it would
    /// take over or lose that writer's flushes.
    #[error("the manifest of day {day} belongs to writer {owner}")]
    ForeignManifest {
        /// The day.
        day: Day,
        /// The writer the manifest belongs to.
        owner: WriterId,
    },
    /// An earlier flush failed after its manifest write started, so storage
    /// may list a flush the checkpoint does not know about. Flushing again
    /// could store the same records twice.
    #[error(
        "an earlier flush failed after writing its manifest; the manifest may list a flush the checkpoint does not know about"
    )]
    NeedsRestart,
}
