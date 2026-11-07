//! Why part of a tree lookup failed.

use crate::manifests::ManifestReadError;
use crate::states_file::TreeReadError;

/// Why part of a tree lookup failed. Each names the day or file it failed
/// on.
#[derive(Debug, thiserror::Error)]
pub enum TreeLookupError {
    /// A day's manifest could not be read.
    #[error(transparent)]
    Manifest(#[from] ManifestReadError),
    /// A states file could not be read.
    #[error(transparent)]
    File(#[from] TreeReadError),
}
