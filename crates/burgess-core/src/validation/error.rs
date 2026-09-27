//! Why a record failed validation.

use super::reference::RefField;

/// Why a decoded record must be skipped.
///
/// One variant per rule, so a log line or a metric points at the rule a
/// producer broke. Positions are character positions in the trimmed value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    /// Nothing is left of the field after trimming whitespace.
    #[error("{field} is empty")]
    Empty {
        /// The empty field.
        field: RefField,
    },
    /// The field has a character its rules do not allow.
    #[error("{field} contains {found:?} at position {position}, which is not allowed there")]
    Character {
        /// The field with the character.
        field: RefField,
        /// Character position.
        position: usize,
        /// The offending character.
        found: char,
    },
    /// The trimmed field is longer than its limit.
    #[error("{field} is {found} bytes long, at most {max} are allowed")]
    TooLong {
        /// The field that is too long.
        field: RefField,
        /// Its length in UTF-8 bytes.
        found: usize,
        /// Its limit in UTF-8 bytes.
        max: usize,
    },
    /// The state has no payload type or one this build does not know, so its
    /// payload cannot be served back.
    #[error("payload type {found} is not set or not known")]
    InvalidPayloadType {
        /// The payload type number as sent.
        found: i32,
    },
    /// The state payload is larger than [`Limits::max_payload_bytes`].
    ///
    /// [`Limits::max_payload_bytes`]: super::Limits::max_payload_bytes
    #[error("payload is {found} bytes, at most {max} are allowed")]
    PayloadTooLarge {
        /// Size of the payload in bytes.
        found: usize,
        /// The configured limit in bytes.
        max: usize,
    },
}
