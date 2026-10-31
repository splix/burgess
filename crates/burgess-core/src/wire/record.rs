//! A record with the checks protobuf cannot express applied.

use prost::Message;

use super::error::DecodeError;
use super::marker::{MARKER, strip_marker};
use super::proto;
use super::proto::record::Body;
use super::unix_millis::UnixMillis;
use crate::flow::{FlowId, TreeId};

/// One observation about one flow, as it travels in a Kafka message.
///
/// Unlike the generated protobuf message, a `Record` always has a well-formed
/// flow id and a body. Everything else is kept as sent: an empty service or an odd time is
/// for validation to judge, not a reason to drop the record while decoding.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// The flow the record belongs to.
    pub flow_id: FlowId,
    /// When it happened, by the producer clock.
    pub time: UnixMillis,
    /// What was observed.
    pub body: Body,
}

impl Record {
    /// Frames the record as the body of one Kafka message.
    pub fn encode(self) -> Vec<u8> {
        let message = proto::Record::from(self);
        let mut bytes = Vec::with_capacity(MARKER.len() + message.encoded_len());
        bytes.extend_from_slice(&MARKER);
        // `encode` would walk the whole message again just to check the buffer
        // has room, which a Vec always has. `encode_raw` is what prost's own
        // `encode_to_vec` uses for the same reason.
        message.encode_raw(&mut bytes);
        bytes
    }

    /// Reads a record from the body of one Kafka message.
    pub fn decode(message: &[u8]) -> Result<Self, DecodeError> {
        let body = strip_marker(message)?;
        let record = proto::Record::decode(body).map_err(DecodeError::UndecodableBody)?;
        Record::try_from(record)
    }

    /// The Kafka partition key. Keying by tree keeps every record of a tree in
    /// one partition.
    pub fn partition_key(&self) -> TreeId {
        self.flow_id.tree()
    }
}

impl TryFrom<proto::Record> for Record {
    type Error = DecodeError;

    fn try_from(record: proto::Record) -> Result<Self, Self::Error> {
        let flow_id = <[u8; 16]>::try_from(record.flow_id.as_slice()).map_err(|_| {
            DecodeError::FlowIdLength {
                found: record.flow_id.len(),
            }
        })?;
        Ok(Record {
            flow_id: FlowId::from(flow_id),
            time: UnixMillis::from(record.ts_ms),
            body: record.body.ok_or(DecodeError::MissingBody)?,
        })
    }
}

impl From<Record> for proto::Record {
    fn from(record: Record) -> Self {
        proto::Record {
            flow_id: record.flow_id.to_bytes().to_vec(),
            ts_ms: record.time.into(),
            body: Some(record.body),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::wire::{PayloadType, Reference, Split, State};

    const FLOW_ID: &str = "0192f3a1b2c3d4e5f6a7b8c900000000";

    fn record(body: Body) -> Record {
        Record {
            flow_id: FLOW_ID.parse().unwrap(),
            time: UnixMillis::from(1_790_000_000_123),
            body,
        }
    }

    fn state() -> Body {
        Body::State(State {
            service: "checkout".into(),
            category: "payment".into(),
            meta: BTreeMap::from([
                ("currency".into(), "EUR".into()),
                ("region".into(), "eu-west".into()),
            ]),
            payload_type: PayloadType::Json.into(),
            payload: br#"{"amount": 42}"#.to_vec(),
        })
    }

    fn reference(category: Option<&str>) -> Body {
        Body::Reference(Reference {
            ref_type: "order_id".into(),
            domain_id: "ORD-1001".into(),
            service: "orders".into(),
            category: category.map(Into::into),
        })
    }

    /// Frames a raw protobuf record, so tests can send what `Record` would
    /// never encode.
    fn frame(record: proto::Record) -> Vec<u8> {
        [&MARKER[..], &record.encode_to_vec()].concat()
    }

    fn assert_round_trip(record: Record) {
        assert_eq!(Record::decode(&record.clone().encode()), Ok(record));
    }

    #[test]
    fn state_round_trip() {
        assert_round_trip(record(state()));
    }

    #[test]
    fn split_round_trip() {
        assert_round_trip(record(Body::Split(Split {
            service: "checkout".into(),
            category: "fan-out".into(),
            child_subflow: 0xdead_beef,
        })));
    }

    #[test]
    fn reference_with_category_round_trip() {
        assert_round_trip(record(reference(Some("lookup"))));
    }

    #[test]
    fn reference_without_category_round_trip() {
        assert_round_trip(record(reference(None)));
    }

    #[test]
    fn message_starts_with_marker() {
        assert_eq!(record(state()).encode()[..4], *b"BGS1");
    }

    #[test]
    fn same_record_encodes_to_same_bytes() {
        assert_eq!(record(state()).encode(), record(state()).encode());
    }

    #[test]
    fn partition_key_is_the_tree() {
        let record = record(state());
        assert_eq!(record.partition_key(), record.flow_id.tree());
    }

    #[test]
    fn keeps_what_validation_should_judge() {
        let odd = Record {
            flow_id: FLOW_ID.parse().unwrap(),
            time: UnixMillis::from(u64::MAX),
            body: Body::Split(Split {
                service: String::new(),
                category: String::new(),
                child_subflow: 0,
            }),
        };
        assert_round_trip(odd);
    }

    #[test]
    fn payload_type_reads_back_typed() {
        let message = record(state()).encode();
        let Ok(Record {
            body: Body::State(state),
            ..
        }) = Record::decode(&message)
        else {
            panic!("expected a state record");
        };
        assert_eq!(state.payload_type(), PayloadType::Json);
    }

    #[test]
    fn keeps_unset_and_unknown_payload_types() {
        for payload_type in [PayloadType::Unspecified.into(), 99] {
            assert_round_trip(record(Body::State(State {
                payload_type,
                ..State::default()
            })));
        }
    }

    #[test]
    fn rejects_unknown_marker() {
        let mut message = record(state()).encode();
        message[..4].copy_from_slice(b"XYZ1");
        assert_eq!(
            Record::decode(&message),
            Err(DecodeError::UnknownMarker {
                found: b"XYZ1".to_vec()
            })
        );
    }

    #[test]
    fn rejects_unknown_framing_version() {
        let mut message = record(state()).encode();
        message[3] = b'2';
        assert_eq!(
            Record::decode(&message),
            Err(DecodeError::UnknownFramingVersion { version: b'2' })
        );
    }

    #[test]
    fn rejects_undecodable_body() {
        let message = [&MARKER[..], &[0xff; 8]].concat();
        assert!(matches!(
            Record::decode(&message),
            Err(DecodeError::UndecodableBody(_))
        ));
    }

    #[test]
    fn rejects_flow_id_of_wrong_length() {
        let message = frame(proto::Record {
            flow_id: vec![0; 15],
            ts_ms: 1,
            body: Some(state()),
        });
        assert_eq!(
            Record::decode(&message),
            Err(DecodeError::FlowIdLength { found: 15 })
        );
    }

    #[test]
    fn rejects_missing_body() {
        let message = frame(proto::Record {
            flow_id: vec![0; 16],
            ts_ms: 1,
            body: None,
        });
        assert_eq!(Record::decode(&message), Err(DecodeError::MissingBody));
    }
}
