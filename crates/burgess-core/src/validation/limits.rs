//! The limits a deployment can tune.

/// Size limits a record must fit in.
///
/// Deployments carry payloads of different sizes, so the limits are
/// configuration rather than constants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Largest state payload, in bytes, measured on the payload alone. Meta,
    /// service and the rest of the record do not count against it.
    pub max_payload_bytes: usize,
}

impl Default for Limits {
    /// A payload limit of 1 MiB.
    fn default() -> Self {
        Limits {
            max_payload_bytes: 1024 * 1024,
        }
    }
}
