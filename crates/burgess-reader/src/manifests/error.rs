//! Why a day's manifest could not be read.

use burgess_core::storage::{Day, ManifestError};

/// Why a day's manifest could not be read. A day without a manifest is not
/// one of them: that is a day the writer stored nothing for.
#[derive(Debug, thiserror::Error)]
pub enum ManifestReadError {
    /// The object store failed.
    #[error("reading the manifest of {day} failed")]
    Store {
        /// The day.
        day: Day,
        /// What the store reported.
        #[source]
        source: object_store::Error,
    },
    /// The manifest is not one this reader can use.
    #[error("the manifest of {day} is invalid")]
    Invalid {
        /// The day.
        day: Day,
        /// What is wrong with it.
        #[source]
        source: ManifestError,
    },
    /// The manifest stored under one day describes another, so its files
    /// would be looked for in the wrong place.
    #[error("the manifest stored for {day} describes {found}")]
    WrongDay {
        /// The day whose path the manifest was read from.
        day: Day,
        /// The day the manifest names.
        found: Day,
    },
}
