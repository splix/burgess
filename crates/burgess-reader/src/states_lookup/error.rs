//! Why part of a lookup of states files failed.

use crate::manifests::ManifestReadError;
use crate::states_file::StatesReadError;

/// Why part of a lookup of states files failed: a states lookup, or a tree
/// lookup, which reads the same files. Each names the day or file it failed
/// on.
#[derive(Debug, thiserror::Error)]
pub enum StatesLookupError {
    /// A day's manifest could not be read.
    #[error(transparent)]
    Manifest(#[from] ManifestReadError),
    /// A states file could not be read.
    #[error(transparent)]
    File(#[from] StatesReadError),
}
