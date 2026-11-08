//! Why part of a refs lookup failed.

use crate::manifests::ManifestReadError;
use crate::refs_file::RefsReadError;

/// Why part of a refs lookup failed. Each names the day or file it failed
/// on.
#[derive(Debug, thiserror::Error)]
pub enum RefsLookupError {
    /// A day's manifest could not be read.
    #[error(transparent)]
    Manifest(#[from] ManifestReadError),
    /// A refs file could not be read.
    #[error(transparent)]
    File(#[from] RefsReadError),
}

/// Why a refs lookup cannot start: without `since` the walk ends at the
/// oldest day kept, and the retention reaches past the oldest day that can
/// be named. The walk would then read a manifest for each day back to the
/// year 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("since is necessary, because the retention reaches past the days that can be named")]
pub struct UnboundedWalk;
