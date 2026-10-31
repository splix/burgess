//! The calls an application makes to capture its state.

use std::time::Duration;

use burgess_core::flow::FlowId;
use burgess_core::validation::{Limits, Validate};
use burgess_core::wire::{Body, PayloadType, Record, Reference, Split, State};
use serde::Serialize;

use crate::clock::{Clock, SystemClock};
use crate::dropped::{DroppedRecord, RecordKind};
use crate::sink::Sink;
use crate::throttle::Throttle;

/// Mints flow ids and emits one service's records about them to a [`Sink`].
///
/// None of the calls can fail. A record that cannot be built, or that the
/// writer would skip anyway, is dropped here and logged in the application's
/// own logs, where its developers will see it, instead of travelling to Kafka
/// only to be thrown away.
#[derive(Debug)]
pub struct Emitter<S, C = SystemClock> {
    service: String,
    sink: S,
    clock: C,
    limits: Limits,
    drop_throttle: Throttle,
}

impl<S: Sink> Emitter<S> {
    /// An emitter for `service`, on the host's clock, checking records
    /// against the default [`Limits`].
    ///
    /// Created once when the service starts: every record it emits names
    /// `service`. A process hosting several services creates one emitter for
    /// each, and they can share a sink.
    pub fn new(service: impl Into<String>, sink: S) -> Self {
        Emitter {
            service: service.into(),
            sink,
            clock: SystemClock,
            limits: Limits::default(),
            drop_throttle: Throttle::default(),
        }
    }
}

impl<S: Sink, C: Clock> Emitter<S, C> {
    /// Uses `clock` to stamp records and to mint root flows.
    pub fn with_clock<D: Clock>(self, clock: D) -> Emitter<S, D> {
        Emitter {
            service: self.service,
            sink: self.sink,
            clock,
            limits: self.limits,
            drop_throttle: self.drop_throttle,
        }
    }

    /// Checks records against `limits` before sending them.
    ///
    /// Set them to what the writer is configured with, so that a record is
    /// dropped here exactly when the writer would skip it.
    pub fn with_limits(self, limits: Limits) -> Self {
        Emitter { limits, ..self }
    }

    /// Starts a new tree and returns its root flow.
    ///
    /// Emits nothing: Burgess learns of the tree from the first record about
    /// one of its flows, and the root id already carries the time it started.
    pub fn start(&self) -> FlowId {
        FlowId::mint_root(self.clock.now(), &mut rand::rng())
    }

    /// Mints a child flow of `parent` and records the split.
    ///
    /// Done in one call so the child can never exist without the split that
    /// links it to its parent; without it, a walk down the tree would not
    /// find the child. The child is returned even if the split was dropped,
    /// because the application must carry on either way.
    pub fn split(&self, parent: FlowId, category: &str) -> FlowId {
        let child = parent.new_child();
        self.emit(
            parent,
            Body::Split(Split {
                service: self.service.clone(),
                category: category.into(),
                child_subflow: child.subflow().into(),
            }),
        );
        child
    }

    /// Records a snapshot of application state, with `payload` stored as
    /// JSON.
    pub fn capture<P: Serialize + ?Sized>(&self, flow: FlowId, category: &str, payload: &P) {
        self.capture_with_meta(flow, category, &[], payload);
    }

    /// Like [`Self::capture`], with `meta` as labels that lookups can filter
    /// on by equality. A key given twice keeps its last value.
    pub fn capture_with_meta<P: Serialize + ?Sized>(
        &self,
        flow: FlowId,
        category: &str,
        meta: &[(&str, &str)],
        payload: &P,
    ) {
        let payload = match serde_json::to_vec(payload) {
            Ok(payload) => payload,
            Err(error) => {
                self.log_drop(&DroppedRecord {
                    service: &self.service,
                    category: Some(category),
                    kind: RecordKind::State,
                    flow,
                    reason: error.into(),
                });
                return;
            }
        };
        self.emit(
            flow,
            Body::State(State {
                service: self.service.clone(),
                category: category.into(),
                meta: meta
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
                payload_type: PayloadType::Json.into(),
                payload,
            }),
        );
    }

    /// Links `flow` to an id known outside Burgess, such as an order number,
    /// so an investigator can find the flow from that id.
    ///
    /// Takes no category: what is linked is the flow itself, not a part of
    /// the service's work.
    pub fn reference(&self, flow: FlowId, ref_type: &str, domain_id: &str) {
        self.emit(
            flow,
            Body::Reference(Reference {
                ref_type: ref_type.into(),
                domain_id: domain_id.into(),
                service: self.service.clone(),
                category: None,
            }),
        );
    }

    /// Waits up to `timeout` for the records emitted so far to be delivered.
    ///
    /// Call it when the service shuts down: records the sink still buffers
    /// when it is dropped are lost. See [`Sink::flush`].
    pub fn flush(&self, timeout: Duration) {
        self.sink.flush(timeout);
    }

    fn emit(&self, flow: FlowId, body: Body) {
        let mut record = Record {
            flow_id: flow,
            time: self.clock.now().into(),
            body,
        };
        if let Err(error) = record.validate(&self.limits) {
            self.log_drop(&DroppedRecord::of(&self.service, &record, error.into()));
            return;
        }
        self.sink.send(record.into());
    }

    fn log_drop(&self, dropped: &DroppedRecord) {
        dropped.log(self.drop_throttle.admit(self.clock.now()));
    }
}

#[cfg(test)]
mod tests;
