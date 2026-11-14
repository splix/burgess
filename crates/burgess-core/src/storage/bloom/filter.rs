//! The bits of a bloom filter, and which bits a key sets.

use twox_hash::XxHash3_64;

/// The bits of a filter, and how many of them each key sets.
///
/// The bit positions are part of the stored format, and the writer and the
/// readers are separate builds, thus they must never change within a
/// format version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Filter {
    /// Bit `i` is bit `i % 8` of byte `i / 8`, lowest first.
    bytes: Vec<u8>,
    hash_count: u32,
}

impl Filter {
    /// A filter of `bit_count` bits, a multiple of 8 and not 0, none set.
    pub(super) fn empty(bit_count: u64, hash_count: u32) -> Self {
        let bytes = usize::try_from(bit_count / 8).expect("a filter that fits in memory");
        Filter {
            bytes: vec![0; bytes],
            hash_count,
        }
    }

    /// The filter of stored `bytes`, not empty.
    pub(super) fn from_bytes(bytes: Vec<u8>, hash_count: u32) -> Self {
        Filter { bytes, hash_count }
    }

    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn bit_count(&self) -> u64 {
        self.bytes.len() as u64 * 8
    }

    pub(super) fn hash_count(&self) -> u32 {
        self.hash_count
    }

    pub(super) fn insert(&mut self, key: &[u8]) {
        for bit in self.positions(key) {
            self.bytes[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    /// Whether `key` can be in the filter. A key that was inserted always
    /// is.
    pub(super) fn may_contain(&self, key: &[u8]) -> bool {
        self.positions(key)
            .all(|bit| self.bytes[(bit / 8) as usize] & (1 << (bit % 8)) != 0)
    }

    /// The part of the bits that are set.
    pub(super) fn fill_ratio(&self) -> f64 {
        let set: u64 = self
            .bytes
            .iter()
            .map(|byte| u64::from(byte.count_ones()))
            .sum();
        set as f64 / self.bit_count() as f64
    }

    /// The bits that `key` sets: two hashes make all of them, which is as
    /// good as one hash for each bit.
    ///
    /// The format sets bit `(h0 + i * h1) mod bits`. Each term is reduced
    /// first, thus the next bit is one addition away, without a division
    /// for each bit: the filter sets several bits for each key of a flush.
    fn positions(&self, key: &[u8]) -> impl Iterator<Item = u64> + use<> {
        let bits = self.bit_count();
        let first = XxHash3_64::oneshot_with_seed(0, key) % bits;
        let step = XxHash3_64::oneshot_with_seed(1, key) % bits;
        // Both are below `bits`, which is far below 2^63, thus the sum fits.
        std::iter::successors(Some(first), move |bit| {
            let next = bit + step;
            Some(if next >= bits { next - bits } else { next })
        })
        .take(self.hash_count as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The positions of format version 1, computed with the Python
    /// `xxhash` package, thus not by this code.
    #[test]
    fn a_key_sets_the_bits_of_the_format() {
        let filter = Filter::empty(65_536, 7);
        let positions: Vec<u64> = filter.positions(b"order_id\x00ord-1").collect();
        assert_eq!(positions, PINNED);
    }

    const PINNED: [u64; 7] = [57_010, 48_864, 40_718, 32_572, 24_426, 16_280, 8_134];

    #[test]
    fn bits_are_lowest_first_in_each_byte() {
        let mut filter = Filter::empty(16, 1);
        filter.insert(b"order_id\x00ord-1");
        let bit = filter.positions(b"order_id\x00ord-1").next().unwrap();
        let mut expected = vec![0u8; 2];
        expected[(bit / 8) as usize] = 1 << (bit % 8);
        assert_eq!(filter.bytes(), expected);
    }
}
