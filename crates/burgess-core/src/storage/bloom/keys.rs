//! The keys of the bloom filters, as bytes.
//!
//! The writer inserts these bytes and the query service probes with them,
//! thus a change here makes the filters of the stored days answer "not
//! here" for keys they hold.

use crate::flow::TreeId;
use crate::storage::day::Day;
use crate::storage::object_name::ObjectName;
use crate::wire::UnixMillis;

/// A key that a filter of one kind holds.
pub trait BloomKey {
    /// The magic of the objects of the filters of this kind. Each kind has
    /// its own, thus a filter read for the other kind is refused: its
    /// probes would say "no" for keys that are there.
    const MAGIC: [u8; 4];

    /// What logs call the filters of this kind.
    const KIND: &'static str;

    /// The name of the filter of this kind of `day`.
    fn object(day: Day) -> ObjectName;

    /// The bytes that the filter hashes.
    fn bytes(&self) -> &[u8];
}

/// Separates the parts of a key. A reference type has no `0x00`, thus the
/// parts cannot run into each other.
const SEPARATOR: u8 = 0x00;

/// A key of the references filter: the pair of a reference type and an
/// outside id, for the day or for one file of it.
///
/// Give the values as stored, as `RefField::normalize` leaves them. The
/// writer stores them so, and a lookup normalizes its values the same way,
/// thus both make the same key. The key lowercases ASCII letters only, so
/// that it does not depend on the Unicode tables of a build. The values
/// stay case-sensitive: a key of other case only finds files to read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RefsKey(Vec<u8>);

impl RefsKey {
    /// The key that tells whether the day holds the reference.
    pub fn day(ref_type: &str, domain_id: &str) -> Self {
        let mut key = Vec::with_capacity(ref_type.len() + 1 + domain_id.len());
        push_pair(&mut key, ref_type, domain_id);
        RefsKey(key)
    }

    /// The key that tells whether the refs file of the flush `nnnn` holds
    /// the reference.
    pub fn file(nnnn: UnixMillis, ref_type: &str, domain_id: &str) -> Self {
        let mut key = Vec::with_capacity(FILE_PREFIX_LEN + ref_type.len() + 1 + domain_id.len());
        key.extend_from_slice(&file_prefix(nnnn));
        push_pair(&mut key, ref_type, domain_id);
        RefsKey(key)
    }
}

impl BloomKey for RefsKey {
    const MAGIC: [u8; 4] = *b"BGSB";
    const KIND: &'static str = "refs";

    fn object(day: Day) -> ObjectName {
        ObjectName::RefsBloom { day }
    }

    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A key of the trees filter: a tree in the states file of one flush.
///
/// The filter has no key for the day: the tree bounds of the manifest tell
/// exactly whether a day can hold a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TreeKey([u8; FILE_PREFIX_LEN + 12]);

impl TreeKey {
    /// The key that tells whether the states file of the flush `nnnn` holds
    /// rows of `tree`.
    pub fn file(nnnn: UnixMillis, tree: TreeId) -> Self {
        let mut key = [0; FILE_PREFIX_LEN + 12];
        let (prefix, id) = key.split_at_mut(FILE_PREFIX_LEN);
        prefix.copy_from_slice(&file_prefix(nnnn));
        id.copy_from_slice(tree.as_bytes());
        TreeKey(key)
    }
}

impl BloomKey for TreeKey {
    const MAGIC: [u8; 4] = *b"BGSC";
    const KIND: &'static str = "trees";

    fn object(day: Day) -> ObjectName {
        ObjectName::TreesBloom { day }
    }

    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

const FILE_PREFIX_LEN: usize = 9;

/// The flush time that names the file, big-endian, and the separator.
fn file_prefix(nnnn: UnixMillis) -> [u8; FILE_PREFIX_LEN] {
    let mut prefix = [SEPARATOR; FILE_PREFIX_LEN];
    prefix[..8].copy_from_slice(&u64::from(nnnn).to_be_bytes());
    prefix
}

fn push_pair(key: &mut Vec<u8>, ref_type: &str, domain_id: &str) {
    key.extend(ref_type.bytes().map(|byte| byte.to_ascii_lowercase()));
    key.push(SEPARATOR);
    key.extend(domain_id.bytes().map(|byte| byte.to_ascii_lowercase()));
}

#[cfg(test)]
mod tests {
    use super::*;

    const NNNN: u64 = 0x0102_0304_0506_0708;

    #[test]
    fn a_day_key_is_the_lowercase_pair() {
        assert_eq!(
            RefsKey::day("Order_ID", "ORD-1").bytes(),
            b"order_id\x00ord-1"
        );
    }

    #[test]
    fn a_file_key_starts_with_the_flush_time() {
        assert_eq!(
            RefsKey::file(UnixMillis::from(NNNN), "order_id", "ORD-1").bytes(),
            b"\x01\x02\x03\x04\x05\x06\x07\x08\x00order_id\x00ord-1"
        );
        let tree = TreeId::from([0xaa; 12]);
        let mut expected = b"\x01\x02\x03\x04\x05\x06\x07\x08\x00".to_vec();
        expected.extend([0xaa; 12]);
        assert_eq!(
            TreeKey::file(UnixMillis::from(NNNN), tree).bytes(),
            expected
        );
    }

    /// Other letters keep their case and their bytes, whatever the Unicode
    /// tables say.
    #[test]
    fn only_ascii_letters_are_lowercased() {
        assert_eq!(
            RefsKey::day("t", "ÄÖ-Straße-ΣA").bytes(),
            "t\0ÄÖ-straße-Σa".as_bytes()
        );
    }
}
