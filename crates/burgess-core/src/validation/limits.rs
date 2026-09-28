//! The limits a deployment can tune.

use std::num::NonZeroUsize;

/// Size limits a record must fit in.
///
/// Deployments carry payloads of different sizes, so the limits are
/// configuration rather than constants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Largest state payload, in bytes, measured on the payload alone. Meta,
    /// service and the rest of the record do not count against it. Never
    /// zero: that would skip every state, and skipped records are gone.
    pub max_payload_bytes: NonZeroUsize,
}

impl Default for Limits {
    /// A payload limit of 1 MiB.
    fn default() -> Self {
        Limits {
            max_payload_bytes: NonZeroUsize::new(1024 * 1024).expect("not zero"),
        }
    }
}
