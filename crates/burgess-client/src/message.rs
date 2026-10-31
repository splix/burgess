//! A record framed for Kafka.

use burgess_core::flow::TreeId;
use burgess_core::wire::Record;

/// One Kafka message, ready to be produced.
///
/// Built only from a [`Record`], so every sink sends exactly the bytes the
/// writer expects and none of them can get the framing or the key wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    key: TreeId,
    value: Vec<u8>,
}

impl Message {
    /// The partition key: the record's tree, which puts every record of a
    /// tree in the same partition.
    pub fn key(&self) -> &TreeId {
        &self.key
    }

    /// The framed record, as [`Record::decode`] reads it.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

impl From<Record> for Message {
    fn from(record: Record) -> Self {
        Message {
            key: record.partition_key(),
            value: record.encode(),
        }
    }
}
