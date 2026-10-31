//! Which partition a record goes to.

use std::num::NonZeroU32;

use rdkafka::producer::{PARTITION_UA, Partitioner};

use super::tree_of_key;

/// Puts every record in its tree's partition as [`TreeId::partition`]
/// computes it, instead of hashing the key the way librdkafka does by
/// default, which other Kafka clients do not reproduce.
///
/// An unavailable partition is not swapped for another, because that would
/// split the tree. Its records wait, or are dropped like any other record
/// Kafka does not take.
///
/// [`TreeId::partition`]: burgess_core::flow::TreeId::partition
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct TreePartitioner;

impl Partitioner for TreePartitioner {
    fn partition(
        &self,
        _topic_name: &str,
        key: Option<&[u8]>,
        partition_cnt: i32,
        _is_partition_available: impl Fn(i32) -> bool,
    ) -> i32 {
        let count = u32::try_from(partition_cnt).ok().and_then(NonZeroU32::new);
        match (tree_of_key(key), count) {
            (Some(tree), Some(count)) => i32::try_from(tree.partition(count))
                .expect("below the partition count, which is an i32"),
            // The sink keys every message by its tree, so this message was
            // not built by the sink. librdkafka fails it.
            _ => PARTITION_UA,
        }
    }
}

#[cfg(test)]
mod tests {
    use burgess_core::flow::TreeId;

    use super::*;

    const TREE: [u8; 12] = [1, 2, 3, 4, 5, 6, 0, 0, 0, 0, 0, 10];

    fn partition(key: Option<&[u8]>, partition_cnt: i32) -> i32 {
        TreePartitioner.partition("records", key, partition_cnt, |_| true)
    }

    #[test]
    fn uses_the_tree_partition() {
        let expected = TreeId::from(TREE).partition(NonZeroU32::new(4).unwrap());
        assert_eq!(partition(Some(&TREE), 4), expected as i32);
    }

    #[test]
    fn keeps_the_partition_while_it_is_unavailable() {
        let partition = TreePartitioner.partition("records", Some(&TREE), 4, |_| false);
        assert_eq!(partition, 10 % 4);
    }

    #[test]
    fn leaves_keys_that_are_not_trees_unassigned() {
        assert_eq!(partition(None, 4), PARTITION_UA);
        assert_eq!(partition(Some(&TREE[..11]), 4), PARTITION_UA);
    }

    #[test]
    fn leaves_topics_without_partitions_unassigned() {
        assert_eq!(partition(Some(&TREE), 0), PARTITION_UA);
    }
}
