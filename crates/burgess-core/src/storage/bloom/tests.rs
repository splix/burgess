//! A filter never says "no" for a key it holds, says "maybe" for others at
//! about its target rate, and reads back as it was stored.

use std::num::NonZeroU64;

use jiff::civil::date;

use super::*;
use crate::flow::TreeId;
use crate::storage::day::Day;
use crate::storage::frame::{self, FrameError};
use crate::storage::proto;
use crate::wire::UnixMillis;

const NNNN: u64 = 1_794_650_000_123;

fn day() -> Day {
    Day::try_from(date(2026, 11, 14)).unwrap()
}

fn size(capacity: u64, rate: f64, min_bytes: u64) -> BloomSize {
    BloomSize {
        capacity: NonZeroU64::new(capacity).unwrap(),
        target_fpp: Fpp::try_from(rate).unwrap(),
        min_bytes,
    }
}

fn refs(size: BloomSize) -> RefsBloom {
    RefsBloom::new(day(), "writer-1".parse().unwrap(), size).unwrap()
}

fn trees(size: BloomSize) -> TreesBloom {
    TreesBloom::new(day(), "writer-1".parse().unwrap(), size).unwrap()
}

/// The keys of one refs file, as a day holds them: one flush time, and ids
/// that differ in a few characters.
fn order(id: u32) -> RefsKey {
    RefsKey::file(UnixMillis::from(NNNN), "order_id", &format!("ORD-{id}"))
}

/// The part of `probes` that the filter says "maybe" for.
fn measured_rate(filter: &RefsBloom, probes: impl Iterator<Item = RefsKey>) -> f64 {
    let (mut maybe, mut all) = (0, 0);
    for key in probes {
        all += 1;
        maybe += usize::from(filter.may_contain(&key));
    }
    maybe as f64 / all as f64
}

const KEYS: u32 = 100_000;

#[test]
fn a_filter_at_its_capacity_holds_each_key_and_keeps_its_rate() {
    let mut filter = refs(size(u64::from(KEYS), 0.01, 0));
    for id in 0..KEYS {
        filter.insert(&order(id));
    }

    assert!((0..KEYS).all(|id| filter.may_contain(&order(id))));
    let rate = measured_rate(&filter, (KEYS..2 * KEYS).map(order));
    // 100,000 probes measure the rate to about 0.0003. A filter of the
    // wrong size, as with bits taken for bytes, is far out on either side.
    assert!((0.008..0.012).contains(&rate), "{rate}");
}

#[test]
fn a_trees_filter_holds_each_tree_and_keeps_its_rate() {
    let tree = |n: u32| {
        let mut id = [0x19; 12];
        id[8..].copy_from_slice(&n.to_be_bytes());
        TreeKey::file(UnixMillis::from(NNNN), TreeId::from(id))
    };
    let mut filter = trees(size(10_000, 0.01, 0));
    for n in 0..10_000 {
        filter.insert(&tree(n));
    }

    assert!((0..10_000).all(|n| filter.may_contain(&tree(n))));
    let maybe = (10_000..110_000)
        .filter(|n| filter.may_contain(&tree(*n)))
        .count();
    let rate = maybe as f64 / 100_000.0;
    assert!((0.008..0.012).contains(&rate), "{rate}");
}

/// A day with few keys gets a filter of the floor size, thus a lower rate,
/// with the hashes of its target rate.
#[test]
fn a_filter_at_its_floor_size_has_a_lower_rate() {
    let mut filter = refs(size(1_000, 0.01, 1 << 20));
    for id in 0..1_000 {
        filter.insert(&order(id));
    }

    assert_eq!(filter.encode().len() - header_len(&filter), 1 << 20);
    assert!((0..1_000).all(|id| filter.may_contain(&order(id))));
    let rate = measured_rate(&filter, (KEYS..2 * KEYS).map(order));
    assert!(rate < 0.001, "{rate}");
}

/// The bytes before the filter bits of the object of `filter`.
fn header_len(filter: &RefsBloom) -> usize {
    let bytes = filter.encode();
    let (_, bits) = frame::decode::<proto::BloomHeader>(*b"BGSB", &bytes).unwrap();
    bytes.len() - bits.len()
}

#[test]
fn a_key_of_another_file_or_day_is_another_key() {
    let mut filter = refs(size(100, 0.000_001, 0));
    filter.insert(&order(1));

    assert!(filter.may_contain(&order(1)));
    assert!(!filter.may_contain(&RefsKey::day("order_id", "ORD-1")));
    assert!(!filter.may_contain(&RefsKey::file(
        UnixMillis::from(NNNN + 1),
        "order_id",
        "ORD-1"
    )));
}

/// A reference in another case can be in the day, thus it is probed as the
/// same key.
#[test]
fn a_key_finds_the_reference_in_any_ascii_case() {
    let mut filter = refs(size(100, 0.01, 0));
    filter.insert(&RefsKey::day("order_id", "ord-1"));

    assert!(filter.may_contain(&RefsKey::day("ORDER_ID", "ORD-1")));
}

/// The writer adds the keys of each flush to the filter that it stored.
#[test]
fn a_stored_filter_holds_its_keys_and_takes_more() {
    let mut filter = refs(size(1_000, 0.01, 0));
    for id in 0..500 {
        filter.insert(&order(id));
    }

    let mut stored = RefsBloom::decode(&filter.encode()).unwrap();
    for id in 500..1_000 {
        stored.insert(&order(id));
    }

    assert!((0..1_000).all(|id| stored.may_contain(&order(id))));
    assert_eq!(stored.insertions(), 1_000);
    assert_eq!(stored.capacity(), filter.capacity());
}

#[test]
fn a_filter_reads_back_as_it_was_stored() {
    let mut filter = refs(size(1_000, 0.01, 0));
    for id in 0..100 {
        filter.insert(&order(id));
    }

    assert_eq!(RefsBloom::decode(&filter.encode()).unwrap(), filter);
}

/// A filter of one key, computed with the Python `xxhash` package and a
/// protobuf encoding by hand, thus not by this code.
#[test]
fn a_filter_is_stored_in_the_format() {
    let mut filter = refs(size(1, 0.01, 0));
    filter.insert(&RefsKey::day("order_id", "ORD-1"));

    let expected = "42475342000000300801120832303236313131341a087772697465722d31\
                    2007281030013801417b14ae47e17a843f49000000000000dc3f4555";
    assert_eq!(hex(&filter.encode()), expected);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn valid_header(filter: &RefsBloom) -> proto::BloomHeader {
    let bytes = filter.encode();
    let (header, _) = frame::decode::<proto::BloomHeader>(*b"BGSB", &bytes).unwrap();
    header
}

/// `header` framed as a references filter with `bits` after it.
fn stored(header: &proto::BloomHeader, bits: &[u8]) -> Vec<u8> {
    frame::encode(*b"BGSB", header, bits)
}

/// A header that a probe cannot use is refused, not read into a filter
/// that fails, hangs, or says "no" for a key it holds.
#[test]
fn a_header_that_a_probe_cannot_use_is_refused() {
    let filter = refs(size(100, 0.01, 0));
    let valid = valid_header(&filter);
    let bits = vec![0; (valid.bit_count / 8) as usize];
    let refused = |header: proto::BloomHeader, bits: &[u8]| {
        RefsBloom::decode(&stored(&header, bits)).expect_err("refused")
    };

    assert_eq!(
        refused(
            proto::BloomHeader {
                format_version: 2,
                ..valid.clone()
            },
            &bits
        ),
        BloomError::UnsupportedVersion {
            found: 2,
            expected: 1
        }
    );
    assert_eq!(
        refused(
            proto::BloomHeader {
                bit_count: 0,
                ..valid.clone()
            },
            &[]
        ),
        BloomError::NoBits
    );
    assert_eq!(
        refused(
            proto::BloomHeader {
                bit_count: valid.bit_count + 8,
                ..valid.clone()
            },
            &bits
        ),
        BloomError::BitCount {
            header: valid.bit_count + 8,
            filter: valid.bit_count
        }
    );
    for hash_count in [0, 33, u32::MAX] {
        assert_eq!(
            refused(
                proto::BloomHeader {
                    hash_count,
                    ..valid.clone()
                },
                &bits
            ),
            BloomError::HashCount { found: hash_count }
        );
    }
    assert_eq!(
        refused(
            proto::BloomHeader {
                capacity: 0,
                ..valid.clone()
            },
            &bits
        ),
        BloomError::NoCapacity
    );
    for target_fpp in [f64::NAN, 0.0, 1.0, -0.5, f64::INFINITY] {
        let error = refused(
            proto::BloomHeader {
                target_fpp,
                ..valid.clone()
            },
            &bits,
        );
        assert!(
            matches!(error, BloomError::TargetFpp(_)),
            "{target_fpp}: {error:?}"
        );
    }
}

#[test]
fn an_object_of_another_kind_is_refused() {
    let filter = refs(size(100, 0.01, 0));
    let mut bytes = filter.encode();
    bytes[..4].copy_from_slice(b"BGSM");

    assert!(matches!(
        RefsBloom::decode(&bytes),
        Err(BloomError::Frame(FrameError::WrongMagic { .. }))
    ));
}

/// The probes of one kind of key in a filter of the other say "no" for
/// keys that are there, thus the objects of the kinds cannot be mixed up.
#[test]
fn a_filter_of_the_other_kind_is_refused() {
    let refs = refs(size(100, 0.01, 0)).encode();
    let trees = trees(size(100, 0.01, 0)).encode();

    assert_eq!(&refs[..4], b"BGSB");
    assert_eq!(&trees[..4], b"BGSC");
    assert!(matches!(
        TreesBloom::decode(&refs),
        Err(BloomError::Frame(FrameError::WrongMagic { .. }))
    ));
    assert!(matches!(
        RefsBloom::decode(&trees),
        Err(BloomError::Frame(FrameError::WrongMagic { .. }))
    ));
}

/// A wrong size, as from a damaged count of keys, is refused before it is
/// allocated.
#[test]
fn a_filter_larger_than_the_maximum_is_refused() {
    let new = |size: BloomSize| RefsBloom::new(day(), "writer-1".parse().unwrap(), size);
    let at_most = size(1, 0.01, MAX_BLOOM_BYTES);
    let too_many_keys = size(u64::MAX, 0.01, 0);
    let too_large_floor = size(1, 0.01, MAX_BLOOM_BYTES + 1);

    // Checked without the filter, which would allocate a gigabyte.
    assert_eq!(at_most.bit_count(), Ok(MAX_BLOOM_BYTES * 8));
    assert!(matches!(
        new(too_many_keys),
        Err(BloomError::TooLarge {
            max: MAX_BLOOM_BYTES,
            ..
        })
    ));
    assert_eq!(
        new(too_large_floor).unwrap_err(),
        BloomError::TooLarge {
            bytes: MAX_BLOOM_BYTES + 1,
            max: MAX_BLOOM_BYTES
        }
    );
}

#[test]
fn a_rate_is_more_than_0_and_less_than_1() {
    for rate in [0.01, 0.5, 1e-9] {
        assert!(Fpp::try_from(rate).is_ok(), "{rate}");
    }
    for rate in [f64::NAN, 0.0, 1.0, 1e-12, -1.0] {
        assert!(Fpp::try_from(rate).is_err(), "{rate}");
    }
}

/// The hashes depend on the rate only, as a filter of any size keeps the
/// rate with them.
#[test]
fn the_hashes_of_a_filter_come_from_its_rate() {
    for (rate, hashes) in [(0.01, 7), (0.001, 10), (0.5, 1)] {
        let small = refs(size(1_000, rate, 0));
        let floor = refs(size(1_000, rate, 1 << 20));
        assert_eq!(valid_header(&small).hash_count, hashes, "{rate}");
        assert_eq!(valid_header(&floor).hash_count, hashes, "{rate}");
    }
}

/// The bits for 1,000 keys at 1%: about 9.6 for each key.
#[test]
fn a_filter_has_the_bits_that_its_keys_need() {
    let filter = refs(size(1_000, 0.01, 0));
    assert_eq!(valid_header(&filter).bit_count, 9_592);
}

/// A quiet day gets a filter of the floor size, which holds far more keys
/// than the day asked for: 1 MiB holds about 875,000 keys at 1%.
#[test]
fn the_capacity_of_a_filter_is_what_its_bits_hold() {
    let floor = refs(size(10, 0.01, 1 << 20));
    let needed = refs(size(1_000, 0.01, 0));

    assert_eq!(floor.capacity().get(), 875_175);
    assert_eq!(needed.capacity().get(), 1_000);
}

/// A size from a damaged count gets the largest size, not a refusal.
#[test]
fn a_size_over_the_limit_comes_within_it() {
    let usual = size(1_000, 0.01, 1 << 20);
    let too_large = size(u64::MAX, 0.01, MAX_BLOOM_BYTES + 1);

    assert_eq!(usual.within_limit(), usual);
    let within = too_large.within_limit();
    assert_eq!(within.min_bytes, MAX_BLOOM_BYTES);
    assert_eq!(within.bit_count(), Ok(MAX_BLOOM_BYTES * 8));
    // What 1 GiB holds at 1%, less one key for rounding.
    assert_eq!(within.capacity.get(), 896_179_683);
}
