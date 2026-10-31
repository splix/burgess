use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use burgess_core::wire::UnixMillis;
use jiff::Timestamp;
use serde_json::json;

use super::*;
use crate::memory_sink::MemorySink;

const NOW_MS: i64 = 1_790_000_000_123;

struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millisecond(NOW_MS).unwrap()
    }
}

fn emitter() -> (Emitter<Arc<MemorySink>, FixedClock>, Arc<MemorySink>) {
    let sink = Arc::new(MemorySink::default());
    (
        Emitter::new("checkout", sink.clone()).with_clock(FixedClock),
        sink,
    )
}

/// Reads back the only message sent, checking it is keyed by its tree.
fn only_record(sink: &MemorySink) -> Record {
    let messages = sink.take();
    let [message] = messages.as_slice() else {
        panic!("expected one message, got {}", messages.len());
    };
    let record = Record::decode(message.value()).unwrap();
    assert_eq!(*message.key(), record.flow_id.tree());
    record
}

fn expected(flow_id: FlowId, body: Body) -> Record {
    Record {
        flow_id,
        time: UnixMillis::from(NOW_MS as u64),
        body,
    }
}

#[test]
fn root_starts_now_and_emits_nothing() {
    let (emitter, sink) = emitter();
    let root = emitter.start();
    assert!(root.is_root());
    assert_eq!(root.started_at(), FixedClock.now());
    assert_eq!(sink.take(), vec![]);
}

#[test]
fn split_returns_child_in_parent_tree() {
    let (emitter, _sink) = emitter();
    let root = emitter.start();
    let child = emitter.split(root, "fan-out");
    assert_eq!(child.tree(), root.tree());
    assert!(!child.is_root());
}

#[test]
fn split_is_recorded_on_the_parent() {
    let (emitter, sink) = emitter();
    let root = emitter.start();
    let child = emitter.split(root, "fan-out");
    assert_eq!(
        only_record(&sink),
        expected(
            root,
            Body::Split(Split {
                service: "checkout".into(),
                category: "fan-out".into(),
                child_subflow: child.subflow().into(),
            })
        )
    );
}

#[test]
fn capture_with_meta_carries_payload_and_meta() {
    let (emitter, sink) = emitter();
    let flow = emitter.start();
    emitter.capture_with_meta(
        flow,
        "payment",
        &[("currency", "EUR"), ("region", "eu-west")],
        &json!({"amount": 42}),
    );
    assert_eq!(
        only_record(&sink),
        expected(
            flow,
            Body::State(State {
                service: "checkout".into(),
                category: "payment".into(),
                meta: BTreeMap::from([
                    ("currency".into(), "EUR".into()),
                    ("region".into(), "eu-west".into()),
                ]),
                payload_type: PayloadType::Json.into(),
                payload: br#"{"amount":42}"#.to_vec(),
            })
        )
    );
}

#[test]
fn capture_has_no_meta() {
    let (emitter, sink) = emitter();
    emitter.capture(emitter.start(), "payment", &json!({"amount": 42}));
    let Body::State(state) = only_record(&sink).body else {
        panic!("expected a state");
    };
    assert_eq!(
        (state.meta, state.payload),
        (BTreeMap::new(), br#"{"amount":42}"#.to_vec())
    );
}

#[test]
fn reference_links_the_flow() {
    let (emitter, sink) = emitter();
    let flow = emitter.start();
    emitter.reference(flow, "order_id", "ORD-1001");
    assert_eq!(
        only_record(&sink),
        expected(
            flow,
            Body::Reference(Reference {
                ref_type: "order_id".into(),
                domain_id: "ORD-1001".into(),
                service: "checkout".into(),
                category: None,
            })
        )
    );
}

#[test]
fn reference_is_sent_trimmed() {
    let (emitter, sink) = emitter();
    emitter.reference(emitter.start(), " order_id ", " ORD-1 ");
    let Body::Reference(reference) = only_record(&sink).body else {
        panic!("expected a reference");
    };
    assert_eq!(
        (reference.ref_type.as_str(), reference.domain_id.as_str()),
        ("order_id", "ORD-1")
    );
}

#[test]
fn drops_reference_the_writer_would_skip() {
    let (emitter, sink) = emitter();
    emitter.reference(emitter.start(), "order id", "ORD-1");
    assert_eq!(sink.take(), vec![]);
}

#[test]
fn drops_state_over_the_payload_limit() {
    let (emitter, sink) = emitter();
    let emitter = emitter.with_limits(Limits {
        max_payload_bytes: NonZeroUsize::new(4).unwrap(),
    });
    let flow = emitter.start();
    emitter.capture(flow, "payment", "ok");
    emitter.capture(flow, "payment", "too long");
    let Body::State(state) = only_record(&sink).body else {
        panic!("expected a state");
    };
    assert_eq!(state.payload, br#""ok""#);
}

#[test]
fn drops_state_whose_payload_is_not_json() {
    let (emitter, sink) = emitter();
    let not_json = BTreeMap::from([((1, 2), "tuple keys have no JSON form")]);
    emitter.capture(emitter.start(), "payment", &not_json);
    assert_eq!(sink.take(), vec![]);
}

#[test]
fn records_of_a_tree_share_a_key() {
    let (emitter, sink) = emitter();
    let root = emitter.start();
    let child = emitter.split(root, "fan-out");
    emitter.capture(child, "charge", &json!({}));
    let keys: Vec<_> = sink
        .take()
        .into_iter()
        .map(|message| *message.key())
        .collect();
    assert_eq!(keys, vec![root.tree(), root.tree()]);
}
