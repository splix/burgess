use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId};

use burgess_core::validation::Limits;
use burgess_core::wire::{Body, PayloadType, Record, Reference, Split, State, UnixMillis};
use jiff::Timestamp;
use serde::{Serialize, Serializer};
use serde_json::json;

use super::*;
use crate::memory_sink::MemorySink;
use crate::message::Message;

const NOW_MS: i64 = 1_790_000_000_123;
const PATIENCE: Duration = Duration::from_secs(10);

struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millisecond(NOW_MS).unwrap()
    }
}

fn builder() -> EmitterBuilder {
    Emitter::builder("checkout").clock(FixedClock)
}

fn emitter() -> (Emitter, Arc<MemorySink>) {
    let sink = Arc::new(MemorySink::default());
    (builder().start(sink.clone()), sink)
}

/// The messages that reached the sink once everything emitted so far did.
fn sent(emitter: &Emitter, sink: &MemorySink) -> Vec<Message> {
    emitter.flush(PATIENCE);
    sink.take()
}

/// Reads back the only message sent, checking it is keyed by its tree.
fn only_record(emitter: &Emitter, sink: &MemorySink) -> Record {
    let messages = sent(emitter, sink);
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
    assert_eq!(sent(&emitter, &sink), vec![]);
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
        only_record(&emitter, &sink),
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
        json!({"amount": 42}),
    );
    assert_eq!(
        only_record(&emitter, &sink),
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
    emitter.capture(emitter.start(), "payment", json!({"amount": 42}));
    let Body::State(state) = only_record(&emitter, &sink).body else {
        panic!("expected a state");
    };
    assert_eq!(
        (state.meta, state.payload),
        (BTreeMap::new(), br#"{"amount":42}"#.to_vec())
    );
}

#[test]
fn capture_takes_shared_state() {
    let (emitter, sink) = emitter();
    let state = Arc::new(json!({"amount": 42}));
    emitter.capture(emitter.start(), "payment", state.clone());
    let Body::State(captured) = only_record(&emitter, &sink).body else {
        panic!("expected a state");
    };
    assert_eq!(captured.payload, serde_json::to_vec(&*state).unwrap());
}

/// Notes the thread it is written as JSON on.
struct ThreadProbe(Arc<Mutex<Option<ThreadId>>>);

impl Serialize for ThreadProbe {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        *self.0.lock().unwrap() = Some(thread::current().id());
        serializer.serialize_unit()
    }
}

#[test]
fn payload_is_written_off_the_callers_thread() {
    let (emitter, sink) = emitter();
    let written_on = Arc::new(Mutex::new(None));
    emitter.capture(emitter.start(), "payment", ThreadProbe(written_on.clone()));
    assert_eq!(sent(&emitter, &sink).len(), 1);
    let written_on = written_on.lock().unwrap().expect("payload was written");
    assert_ne!(written_on, thread::current().id());
}

#[test]
fn reference_links_the_flow() {
    let (emitter, sink) = emitter();
    let flow = emitter.start();
    emitter.reference(flow, "order_id", "ORD-1001");
    assert_eq!(
        only_record(&emitter, &sink),
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
    let Body::Reference(reference) = only_record(&emitter, &sink).body else {
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
    assert_eq!(sent(&emitter, &sink), vec![]);
}

#[test]
fn drops_state_over_the_payload_limit() {
    let sink = Arc::new(MemorySink::default());
    let emitter = builder()
        .limits(Limits {
            max_payload_bytes: NonZeroUsize::new(4).unwrap(),
        })
        .start(sink.clone());
    let flow = emitter.start();
    emitter.capture(flow, "payment", "ok");
    emitter.capture(flow, "payment", "too long");
    let Body::State(state) = only_record(&emitter, &sink).body else {
        panic!("expected a state");
    };
    assert_eq!(state.payload, br#""ok""#);
}

#[test]
fn drops_state_whose_payload_is_not_json() {
    let (emitter, sink) = emitter();
    let not_json = BTreeMap::from([((1, 2), "tuple keys have no JSON form")]);
    emitter.capture(emitter.start(), "payment", not_json);
    assert_eq!(sent(&emitter, &sink), vec![]);
}

#[test]
fn records_of_a_tree_share_a_key() {
    let (emitter, sink) = emitter();
    let root = emitter.start();
    let child = emitter.split(root, "fan-out");
    emitter.capture(child, "charge", json!({}));
    let keys: Vec<_> = sent(&emitter, &sink)
        .into_iter()
        .map(|message| *message.key())
        .collect();
    assert_eq!(keys, vec![root.tree(), root.tree()]);
}

#[test]
fn dropping_the_emitter_delivers_what_was_queued() {
    let (emitter, sink) = emitter();
    let root = emitter.start();
    for _ in 0..100 {
        emitter.capture(root, "payment", json!({}));
    }
    drop(emitter);
    assert_eq!(sink.take().len(), 100);
}

/// Holds every message until the test lets it through, telling the test
/// when the worker is stuck in `send`.
struct GateSink {
    entered: Mutex<Sender<()>>,
    gate: Mutex<Receiver<()>>,
    passed: MemorySink,
}

impl Sink for GateSink {
    fn send(&self, message: Message) {
        self.entered.lock().unwrap().send(()).unwrap();
        self.gate.lock().unwrap().recv().unwrap();
        self.passed.send(message);
    }
}

#[test]
fn full_queue_drops_new_records_without_waiting() {
    let (entered, worker_in_send) = mpsc::channel();
    let (open, gate) = mpsc::channel();
    let sink = Arc::new(GateSink {
        entered: Mutex::new(entered),
        gate: Mutex::new(gate),
        passed: MemorySink::default(),
    });
    let emitter = builder()
        .capacity(NonZeroUsize::new(1).unwrap())
        .start(sink.clone());
    let root = emitter.start();

    emitter.capture(root, "first", json!({}));
    worker_in_send.recv_timeout(PATIENCE).unwrap();
    // The worker holds the first record, so the second fills the queue and
    // the third has no room.
    emitter.capture(root, "second", json!({}));
    emitter.capture(root, "third", json!({}));

    open.send(()).unwrap();
    open.send(()).unwrap();
    emitter.flush(PATIENCE);
    let categories: Vec<String> = sink
        .passed
        .take()
        .iter()
        .map(
            |message| match Record::decode(message.value()).unwrap().body {
                Body::State(state) => state.category,
                other => panic!("expected a state, got {other:?}"),
            },
        )
        .collect();
    assert_eq!(categories, ["first", "second"]);
}

/// Panics when written as JSON.
struct PanickingPayload;

impl Serialize for PanickingPayload {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        panic!("this payload cannot be written");
    }
}

#[test]
fn panicking_payload_does_not_stop_capture() {
    let (emitter, sink) = emitter();
    let root = emitter.start();
    emitter.capture(root, "broken", PanickingPayload);
    emitter.capture(root, "payment", json!({}));
    let Body::State(state) = only_record(&emitter, &sink).body else {
        panic!("expected a state");
    };
    assert_eq!(state.category, "payment");
}

/// Delivers at once, but its flush waits until the test opens the gate,
/// telling the test when it started waiting.
struct SlowFlushSink {
    flushing: Mutex<Sender<()>>,
    gate: Mutex<Receiver<()>>,
    delivered: MemorySink,
}

impl Sink for SlowFlushSink {
    fn send(&self, message: Message) {
        self.delivered.send(message);
    }

    fn flush(&self, _timeout: Duration) {
        self.flushing.lock().unwrap().send(()).unwrap();
        self.gate.lock().unwrap().recv().unwrap();
    }
}

#[test]
fn slow_sink_flush_does_not_stop_delivery() {
    let (flushing, sink_flushing) = mpsc::channel();
    let (open, gate) = mpsc::channel();
    let sink = Arc::new(SlowFlushSink {
        flushing: Mutex::new(flushing),
        gate: Mutex::new(gate),
        delivered: MemorySink::default(),
    });
    let emitter = Arc::new(builder().start(sink.clone()));
    let flush = thread::spawn({
        let emitter = emitter.clone();
        move || emitter.flush(PATIENCE)
    });
    sink_flushing.recv_timeout(PATIENCE).unwrap();

    // Delivered while the flush above still waits on the sink.
    emitter.capture(emitter.start(), "payment", json!({}));
    let deadline = Instant::now() + PATIENCE;
    while sink.delivered.take().is_empty() {
        assert!(
            Instant::now() < deadline,
            "record not delivered during flush"
        );
        thread::sleep(Duration::from_millis(1));
    }

    open.send(()).unwrap();
    flush.join().unwrap();
}
