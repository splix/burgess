//! Why a Kafka message could not be read as a record.

/// Why a message was not a valid record.
///
/// The writer skips such a message and keeps going, so each reason is its own
/// variant: a log line or a metric can then tell a producer bug apart from
/// traffic that was never meant for Burgess.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The message does not start with the `SCA` magic, or is too short to
    /// hold the full marker.
    #[error("unknown marker \"{}\", expected \"SCA1\"", found.escape_ascii())]
    UnknownMarker {
        /// Up to the first four bytes of the message.
        found: Vec<u8>,
    },
    /// The message starts with `SCA` but the version byte after it is not one
    /// this build can read, most likely a message from a newer producer.
    #[error("unknown framing version \"{}\", expected \"1\"", version.escape_ascii())]
    UnknownFramingVersion {
        /// The byte after `SCA`.
        version: u8,
    },
    /// The bytes after the marker are not a protobuf `Record`.
    #[error("undecodable record body")]
    UndecodableBody(#[source] prost::DecodeError),
    /// The flow id is not exactly 16 bytes.
    #[error("flow id has {found} bytes, expected 16")]
    FlowIdLength {
        /// Length of the flow id in the message.
        found: usize,
    },
    /// The record is none of state, split or reference.
    #[error("record has no body")]
    MissingBody,
}
