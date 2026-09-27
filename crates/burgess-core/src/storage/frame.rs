//! The framing shared by stored protobuf objects.
//!
//! `magic(4) | u32 big-endian length | protobuf | trailing bytes`
//!
//! The magic tells what kind of object a blob is before anything is decoded,
//! and the length lets an object carry raw bytes after its protobuf header,
//! which bloom objects use for their filter.

use prost::Message;

/// Bytes before the protobuf: the magic and the length.
const PREFIX_LEN: usize = 8;

/// Why bytes are not a framed object of the expected kind.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// The object does not start with the expected magic.
    #[error("wrong magic \"{}\", expected \"{}\"", found.escape_ascii(), expected.escape_ascii())]
    WrongMagic {
        /// The magic this kind of object has.
        expected: [u8; 4],
        /// Up to the first four bytes of the object.
        found: Vec<u8>,
    },
    /// The object ends before its protobuf does, or before the length that
    /// says where it ends.
    #[error("object is truncated: {available} bytes, at least {needed} needed")]
    Truncated {
        /// Bytes the frame says the object has at least.
        needed: usize,
        /// Bytes the object actually has.
        available: usize,
    },
    /// The protobuf in the frame cannot be decoded.
    #[error("undecodable protobuf header")]
    UndecodableHeader(#[source] prost::DecodeError),
}

/// Frames `header` with `magic`, followed by `trailing`.
pub(super) fn encode(magic: [u8; 4], header: &impl Message, trailing: &[u8]) -> Vec<u8> {
    let length = header.encoded_len();
    let mut bytes = Vec::with_capacity(PREFIX_LEN + length + trailing.len());
    bytes.extend_from_slice(&magic);
    // Headers are manifests and filter parameters, kilobytes to megabytes;
    // one reaching 4 GiB would mean something far worse has gone wrong.
    let length = u32::try_from(length).expect("protobuf header under 4 GiB");
    bytes.extend_from_slice(&length.to_be_bytes());
    // `encode` would measure the message again just to check the buffer has
    // room, which a Vec always has.
    header.encode_raw(&mut bytes);
    bytes.extend_from_slice(trailing);
    bytes
}

/// Reads a framed object that must start with `magic`, returning its header
/// and the bytes after it.
pub(super) fn decode<M: Message + Default>(
    magic: [u8; 4],
    bytes: &[u8],
) -> Result<(M, &[u8]), FrameError> {
    if bytes.len() < magic.len() && magic.starts_with(bytes) {
        return Err(FrameError::Truncated {
            needed: PREFIX_LEN,
            available: bytes.len(),
        });
    }
    if !bytes.starts_with(&magic) {
        return Err(FrameError::WrongMagic {
            expected: magic,
            found: bytes[..bytes.len().min(magic.len())].to_vec(),
        });
    }
    let Some((&length, rest)) = bytes[magic.len()..].split_first_chunk::<4>() else {
        return Err(FrameError::Truncated {
            needed: PREFIX_LEN,
            available: bytes.len(),
        });
    };
    let length = u32::from_be_bytes(length) as usize;
    if rest.len() < length {
        return Err(FrameError::Truncated {
            needed: PREFIX_LEN.saturating_add(length),
            available: bytes.len(),
        });
    }
    let (header, trailing) = rest.split_at(length);
    let header = M::decode(header).map_err(FrameError::UndecodableHeader)?;
    Ok((header, trailing))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::FlushEntry;

    const MAGIC: [u8; 4] = *b"BGST";

    fn header() -> FlushEntry {
        FlushEntry {
            nnnn: 1_790_000_000_000,
            seq: 7,
            ..FlushEntry::default()
        }
    }

    #[test]
    fn round_trip_with_trailing_bytes() {
        let bytes = encode(MAGIC, &header(), b"filter");
        let (decoded, trailing) = decode::<FlushEntry>(MAGIC, &bytes).unwrap();
        assert_eq!(decoded, header());
        assert_eq!(trailing, b"filter");
    }

    #[test]
    fn layout_is_magic_then_big_endian_length() {
        let bytes = encode(MAGIC, &header(), b"");
        let length = header().encoded_len() as u32;
        assert_eq!(bytes[..4], MAGIC);
        assert_eq!(bytes[4..8], length.to_be_bytes());
        assert_eq!(bytes.len(), 8 + length as usize);
    }

    #[test]
    fn rejects_wrong_magic() {
        let bytes = encode(*b"BGSX", &header(), b"");
        assert_eq!(
            decode::<FlushEntry>(MAGIC, &bytes),
            Err(FrameError::WrongMagic {
                expected: MAGIC,
                found: b"BGSX".to_vec()
            })
        );
    }

    #[test]
    fn object_cut_inside_its_magic_is_truncated() {
        for cut in 0..4 {
            assert_eq!(
                decode::<FlushEntry>(MAGIC, &MAGIC[..cut]),
                Err(FrameError::Truncated {
                    needed: 8,
                    available: cut
                }),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn short_object_of_another_kind_is_wrong_magic() {
        assert!(matches!(
            decode::<FlushEntry>(MAGIC, b"XY"),
            Err(FrameError::WrongMagic { .. })
        ));
    }

    #[test]
    fn rejects_truncated_length() {
        let bytes = encode(MAGIC, &header(), b"");
        assert_eq!(
            decode::<FlushEntry>(MAGIC, &bytes[..6]),
            Err(FrameError::Truncated {
                needed: 8,
                available: 6
            })
        );
    }

    #[test]
    fn rejects_truncated_header() {
        let bytes = encode(MAGIC, &header(), b"");
        let cut = bytes.len() - 1;
        assert_eq!(
            decode::<FlushEntry>(MAGIC, &bytes[..cut]),
            Err(FrameError::Truncated {
                needed: bytes.len(),
                available: cut
            })
        );
    }

    #[test]
    fn rejects_undecodable_header() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&3u32.to_be_bytes());
        bytes.extend_from_slice(&[0xff; 3]);
        assert!(matches!(
            decode::<FlushEntry>(MAGIC, &bytes),
            Err(FrameError::UndecodableHeader(_))
        ));
    }
}
