//! The four bytes in front of every Kafka message.
//!
//! The topic may carry messages that are not Burgess records at all, and a
//! future producer may frame records differently. The marker lets the writer
//! tell both cases apart from a broken record before it tries to decode.

use super::error::DecodeError;

/// Starts every message: the magic `BGS` (for Burgess) followed by the framing version `1`.
///
/// Four literal ASCII bytes, not an integer. The version changes only when the
/// bytes after the marker can no longer be decoded as a `Record`; schema
/// changes use protobuf evolution instead.
pub const MARKER: [u8; 4] = *b"BGS1";

const MAGIC: [u8; 3] = [MARKER[0], MARKER[1], MARKER[2]];
const VERSION: u8 = MARKER[3];

/// Returns the part of `message` after a valid marker.
///
/// A message shorter than the marker is reported as an unknown marker, even
/// when it starts with `BGS`: without its fourth byte there is no version to
/// call unknown.
pub(super) fn strip_marker(message: &[u8]) -> Result<&[u8], DecodeError> {
    let unknown_marker = || DecodeError::UnknownMarker {
        found: message[..message.len().min(MARKER.len())].to_vec(),
    };
    let Some((&[m0, m1, m2, version], body)) = message.split_first_chunk() else {
        return Err(unknown_marker());
    };
    if [m0, m1, m2] != MAGIC {
        return Err(unknown_marker());
    }
    if version != VERSION {
        return Err(DecodeError::UnknownFramingVersion { version });
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_valid_marker() {
        assert_eq!(strip_marker(b"BGS1body"), Ok(&b"body"[..]));
    }

    #[test]
    fn empty_body_is_left_to_the_decoder() {
        assert_eq!(strip_marker(b"BGS1"), Ok(&b""[..]));
    }

    #[test]
    fn foreign_message_is_unknown_marker() {
        assert_eq!(
            strip_marker(b"{\"json\": true}"),
            Err(DecodeError::UnknownMarker {
                found: b"{\"js".to_vec()
            })
        );
    }

    #[test]
    fn newer_version_is_unknown_framing_version() {
        assert_eq!(
            strip_marker(b"BGS2body"),
            Err(DecodeError::UnknownFramingVersion { version: b'2' })
        );
    }

    #[test]
    fn truncated_marker_is_unknown_marker() {
        for message in [&b""[..], b"B", b"BG", b"BGS"] {
            assert_eq!(
                strip_marker(message),
                Err(DecodeError::UnknownMarker {
                    found: message.to_vec()
                }),
                "message {:?}",
                message.escape_ascii().to_string()
            );
        }
    }

    #[test]
    fn error_messages_show_bytes_readably() {
        let marker = DecodeError::UnknownMarker {
            found: vec![0x00, b'B', b'G', 0xff],
        };
        assert_eq!(
            marker.to_string(),
            "unknown marker \"\\x00BG\\xff\", expected \"BGS1\""
        );
        let version = DecodeError::UnknownFramingVersion { version: b'2' };
        assert_eq!(
            version.to_string(),
            "unknown framing version \"2\", expected \"1\""
        );
    }
}
