//! A bloom filter of one day, as the writer stores it.

use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;

use crate::storage::day::Day;
use crate::storage::frame;
use crate::storage::proto;
use crate::storage::writer_id::WriterId;

use super::error::BloomError;
use super::filter::Filter;
use super::keys::{BloomKey, RefsKey, TreeKey};
use super::size::{BloomSize, Fpp, MAX_HASHES};

/// Version of the header's meaning and of the hashing that this build
/// writes and reads.
const FORMAT_VERSION: u32 = 1;

/// A bloom filter of one day, for keys of type `K`.
///
/// It tells which days and files cannot hold a key, thus a lookup reads
/// only the others. It can answer "maybe" for a key that is not there, at
/// about its target rate while it holds no more keys than its capacity, but
/// never "no" for a key that was inserted.
///
/// The writer stores one filter of each kind for each day, and writes it
/// again at each flush with the keys of the flush added.
#[derive(Clone, PartialEq)]
pub struct DayBloom<K> {
    day: Day,
    writer_id: WriterId,
    capacity: NonZeroU64,
    target_fpp: Fpp,
    insertions: u64,
    filter: Filter,
    keys: PhantomData<fn(&K)>,
}

/// The filter of the references of a day and of its refs files.
pub type RefsBloom = DayBloom<RefsKey>;

/// The filter of the trees in the states files of a day.
pub type TreesBloom = DayBloom<TreeKey>;

impl<K: BloomKey> DayBloom<K> {
    /// An empty filter of `day`, of the size that `size` asks for, or why
    /// a filter cannot have that size.
    ///
    /// Its capacity is what its bits hold at the rate, at least the one
    /// asked for: a filter at its floor size holds more keys than a quiet
    /// day asks for.
    pub fn new(day: Day, writer_id: WriterId, size: BloomSize) -> Result<Self, BloomError> {
        let bits = size.bit_count()?;
        let held = size.target_fpp.keys_held(bits).max(size.capacity.get());
        Ok(DayBloom {
            day,
            writer_id,
            capacity: NonZeroU64::new(held).expect("at least the capacity asked for"),
            target_fpp: size.target_fpp,
            insertions: 0,
            filter: Filter::empty(bits, size.target_fpp.hash_count()),
            keys: PhantomData,
        })
    }

    /// Adds `key`. Each call counts as an insertion, thus the writer adds a
    /// key one time, for the count to size the filters of later days.
    pub fn insert(&mut self, key: &K) {
        self.filter.insert(key.bytes());
        // A damaged stored count must not overflow.
        self.insertions = self.insertions.saturating_add(1);
    }

    /// Whether the day or the file of `key` can hold it. `false` is sure.
    pub fn may_contain(&self, key: &K) -> bool {
        self.filter.may_contain(key.bytes())
    }

    /// The day of the filter.
    pub fn day(&self) -> Day {
        self.day
    }

    /// The writer that owns the day.
    pub fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }

    /// The keys that the filter holds at its target rate.
    pub fn capacity(&self) -> NonZeroU64 {
        self.capacity
    }

    /// The false positive rate at the capacity.
    pub fn target_fpp(&self) -> Fpp {
        self.target_fpp
    }

    /// The keys inserted.
    pub fn insertions(&self) -> u64 {
        self.insertions
    }

    /// The part of the bits that are set.
    pub fn fill_ratio(&self) -> f64 {
        self.filter.fill_ratio()
    }

    /// The filter as a framed object, ready to store.
    pub fn encode(&self) -> Vec<u8> {
        let header = proto::BloomHeader {
            format_version: FORMAT_VERSION,
            day: self.day.to_string(),
            writer_id: self.writer_id.to_string(),
            hash_count: self.filter.hash_count(),
            bit_count: self.filter.bit_count(),
            capacity: self.capacity.get(),
            insertion_count: self.insertions,
            target_fpp: self.target_fpp.into(),
            fill_ratio: self.fill_ratio(),
        };
        frame::encode(K::MAGIC, &header, self.filter.bytes())
    }

    /// Reads a stored filter.
    ///
    /// The header comes from storage, thus each value that a probe depends
    /// on is checked: a bad one would make a probe fail, hang or say "no"
    /// for a key that is there.
    pub fn decode(bytes: &[u8]) -> Result<Self, BloomError> {
        let (header, filter) = frame::decode::<proto::BloomHeader>(K::MAGIC, bytes)?;
        if header.format_version != FORMAT_VERSION {
            return Err(BloomError::UnsupportedVersion {
                found: header.format_version,
                expected: FORMAT_VERSION,
            });
        }
        if filter.is_empty() {
            return Err(BloomError::NoBits);
        }
        let filter_bits = filter.len() as u64 * 8;
        if header.bit_count != filter_bits {
            return Err(BloomError::BitCount {
                header: header.bit_count,
                filter: filter_bits,
            });
        }
        if !(1..=MAX_HASHES).contains(&header.hash_count) {
            return Err(BloomError::HashCount {
                found: header.hash_count,
            });
        }
        Ok(DayBloom {
            day: header.day.parse()?,
            writer_id: header.writer_id.parse()?,
            capacity: NonZeroU64::new(header.capacity).ok_or(BloomError::NoCapacity)?,
            target_fpp: Fpp::try_from(header.target_fpp)?,
            insertions: header.insertion_count,
            filter: Filter::from_bytes(filter.to_vec(), header.hash_count),
            keys: PhantomData,
        })
    }
}

/// The bits are left out: a filter has megabytes of them.
impl<K> fmt::Debug for DayBloom<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DayBloom")
            .field("day", &self.day)
            .field("writer_id", &self.writer_id)
            .field("capacity", &self.capacity)
            .field("target_fpp", &self.target_fpp)
            .field("insertions", &self.insertions)
            .field("bit_count", &self.filter.bit_count())
            .field("hash_count", &self.filter.hash_count())
            .finish()
    }
}
