//! The id shared by every flow of one tree.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;

use jiff::Timestamp;
use rand::Rng;

use super::hex::{HexDigits, IdForm, IdParseError, write_hex};

/// Bytes of the root start time at the front of a tree id.
const TIME_BYTES: usize = 6;

/// Identifies a tree of flows: 6 bytes of root start time in unix milliseconds
/// followed by 6 random bytes, big-endian.
///
/// It is also the Kafka partition key, and picks the partition every record
/// of the tree goes to; see [`TreeId::partition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TreeId([u8; 12]);

impl TreeId {
    /// Mints the tree id for a new root flow started at `now`.
    ///
    /// `now` comes from the producer clock. A clock before 1970 is stored as
    /// the epoch instead of failing, because minting an id must never be the
    /// reason an application request fails. No upper bound is needed: the
    /// latest time [`Timestamp`] holds, the end of year 9999, fits in 48 bits.
    pub fn mint<R: Rng + ?Sized>(now: Timestamp, rng: &mut R) -> Self {
        let millis = now.as_millisecond().max(0);
        let mut bytes = [0; 12];
        bytes[..TIME_BYTES].copy_from_slice(&millis.to_be_bytes()[8 - TIME_BYTES..]);
        rng.fill_bytes(&mut bytes[TIME_BYTES..]);
        TreeId(bytes)
    }

    /// When the root flow was minted, by the producer clock.
    ///
    /// A lower bound on the time of any record in the tree, which lets a lookup
    /// skip days that end before the tree started. The prefix of an id that was
    /// not minted here can exceed what [`Timestamp`] holds; such a time is
    /// reported as [`Timestamp::MAX`].
    pub fn started_at(&self) -> Timestamp {
        let mut millis = [0; 8];
        millis[8 - TIME_BYTES..].copy_from_slice(&self.0[..TIME_BYTES]);
        Timestamp::from_millisecond(i64::from_be_bytes(millis)).unwrap_or(Timestamp::MAX)
    }

    /// The Kafka partition, out of `partition_count`, that every record of
    /// the tree goes to, whichever flow of the tree it is about.
    ///
    /// Spelled out here rather than left to a Kafka client's partitioner,
    /// because clients hash keys differently (librdkafka with CRC32, the Java
    /// client with murmur2) and producers in any language must agree. Taken
    /// from the random half of the id, which is already evenly spread; the
    /// time half is the same for every tree started in the same millisecond.
    pub fn partition(&self, partition_count: NonZeroU32) -> u32 {
        let random_half = &self.0[TIME_BYTES..];
        let mut random = [0; 8];
        random[8 - random_half.len()..].copy_from_slice(random_half);
        let partition = u64::from_be_bytes(random) % u64::from(partition_count.get());
        u32::try_from(partition).expect("below the partition count, which is a u32")
    }

    /// The id as it is stored and sent.
    pub fn as_bytes(&self) -> &[u8; 12] {
        &self.0
    }
}

impl From<[u8; 12]> for TreeId {
    fn from(bytes: [u8; 12]) -> Self {
        TreeId(bytes)
    }
}

impl From<TreeId> for [u8; 12] {
    fn from(id: TreeId) -> Self {
        id.0
    }
}

impl fmt::Display for TreeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(f, &self.0)
    }
}

impl FromStr for TreeId {
    type Err = IdParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = HexDigits::parse(text)?;
        digits
            .to_bytes()
            .map(TreeId)
            .ok_or_else(|| digits.wrong_length(IdForm::Tree))
    }
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    fn rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    #[test]
    fn starts_with_mint_time_in_big_endian_millis() {
        let now = Timestamp::from_millisecond(0x0102_0304_0506).unwrap();
        let id = TreeId::mint(now, &mut rng());
        assert_eq!(id.as_bytes()[..6], [1, 2, 3, 4, 5, 6]);
        assert_eq!(id.started_at(), now);
    }

    #[test]
    fn keeps_millisecond_precision_only() {
        let now: Timestamp = "2026-09-27T10:00:00.123456789Z".parse().unwrap();
        let id = TreeId::mint(now, &mut rng());
        assert_eq!(id.started_at().to_string(), "2026-09-27T10:00:00.123Z");
    }

    #[test]
    fn clock_before_epoch_mints_epoch() {
        let now = Timestamp::from_millisecond(-1_000).unwrap();
        let id = TreeId::mint(now, &mut rng());
        assert_eq!(id.started_at(), Timestamp::UNIX_EPOCH);
    }

    #[test]
    fn prefix_beyond_timestamp_range_is_max() {
        let id = TreeId::from([0xff; 12]);
        assert_eq!(id.started_at(), Timestamp::MAX);
    }

    #[test]
    fn same_millisecond_trees_differ() {
        let now = Timestamp::now();
        let mut rng = rng();
        assert_ne!(TreeId::mint(now, &mut rng), TreeId::mint(now, &mut rng));
    }

    #[test]
    fn earlier_tree_sorts_first() {
        let mut rng = rng();
        let earlier = TreeId::mint(Timestamp::from_millisecond(1_000).unwrap(), &mut rng);
        let later = TreeId::mint(Timestamp::from_millisecond(1_001).unwrap(), &mut rng);
        assert!(earlier < later);
    }

    fn partitions(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).unwrap()
    }

    #[test]
    fn partition_is_random_half_modulo_count() {
        let tree = TreeId::from([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 1, 2]);
        assert_eq!(tree.partition(partitions(7)), 0x0102 % 7);
    }

    #[test]
    fn single_partition_takes_every_tree() {
        let tree = TreeId::mint(Timestamp::now(), &mut rng());
        assert_eq!(tree.partition(partitions(1)), 0);
    }

    #[test]
    fn trees_of_one_millisecond_spread_over_partitions() {
        let mut rng = rng();
        let now = Timestamp::now();
        let used: std::collections::BTreeSet<u32> = (0..100)
            .map(|_| TreeId::mint(now, &mut rng).partition(partitions(4)))
            .collect();
        assert_eq!(used.into_iter().collect::<Vec<_>>(), [0, 1, 2, 3]);
    }

    #[test]
    fn text_round_trip() {
        let id = TreeId::mint(Timestamp::now(), &mut rng());
        assert_eq!(id.to_string().parse::<TreeId>().unwrap(), id);
    }

    #[test]
    fn rejects_flow_id_text() {
        let error = "000102030405060708090a0b0c0d0e0f"
            .parse::<TreeId>()
            .unwrap_err();
        assert_eq!(
            error,
            IdParseError::WrongLength {
                expected: IdForm::Tree,
                found: 32
            }
        );
    }
}
