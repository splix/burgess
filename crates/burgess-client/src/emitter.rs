//! The calls an application makes to capture its state.

mod builder;

use std::sync::Arc;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use burgess_core::flow::FlowId;
use serde::Serialize;

pub use builder::EmitterBuilder;

use crate::clock::Clock;
use crate::pending::{Item, Pending, PendingBody};
use crate::queue::Queue;
use crate::sink::Sink;

/// Mints flow ids and emits one service's records about them to a [`Sink`].
///
/// Applications call it from latency-sensitive code, so a call does only
/// what must happen right away: it reads the clock, mints ids, and queues
/// the record without waiting. A background thread writes the payload as
/// JSON, validates, encodes and sends. When that thread falls behind, the
/// queue fills and further records are dropped rather than slowing the
/// caller.
///
/// None of the calls can fail. A record that cannot be built, or that the
/// writer would skip anyway, is dropped by the background thread and logged
/// in the application's own logs, where its developers will see it, instead
/// of travelling to Kafka only to be thrown away.
///
/// Dropping the emitter hands what is still queued to the sink, for up to
/// five seconds, and stops the background thread. Call [`Emitter::flush`]
/// first for the sink to deliver it too.
pub struct Emitter {
    queue: Arc<Queue>,
    clock: Arc<dyn Clock>,
    /// Shared with the worker, so that [`Emitter::flush`] can wait on the
    /// sink itself while the worker keeps draining the queue.
    sink: Arc<dyn Sink>,
    /// `None` when the thread could not be started; records are then queued
    /// until the queue is full, and dropped after.
    worker: Option<JoinHandle<()>>,
}

impl Emitter {
    /// An emitter for `service` with the default settings; see
    /// [`EmitterBuilder`] for the others.
    ///
    /// Created once when the service starts: every record it emits names
    /// `service`. A process hosting several services creates one emitter for
    /// each, and they can share a sink.
    pub fn new(service: impl Into<String>, sink: impl Sink + 'static) -> Self {
        Self::builder(service).start(sink)
    }

    /// Settings for an emitter for `service`, starting from the defaults.
    pub fn builder(service: impl Into<String>) -> EmitterBuilder {
        EmitterBuilder::new(service.into())
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
    /// find the child. The child is returned even if the split is dropped,
    /// because the application must carry on either way.
    pub fn split(&self, parent: FlowId, category: &'static str) -> FlowId {
        let child = parent.new_child();
        self.enqueue(
            parent,
            PendingBody::Split {
                category,
                child: child.subflow(),
            },
        );
        child
    }

    /// Records a snapshot of application state, with `payload` stored as
    /// JSON.
    ///
    /// The payload is moved rather than borrowed, because it is written as
    /// JSON later, on the background thread. Wrap large state in an [`Arc`]
    /// to share it instead of copying it.
    pub fn capture(
        &self,
        flow: FlowId,
        category: &'static str,
        payload: impl Serialize + Send + 'static,
    ) {
        self.capture_with_meta(flow, category, &[], payload);
    }

    /// Like [`Self::capture`], with `meta` as labels that lookups can filter
    /// on by equality. A key given twice keeps its last value.
    ///
    /// The labels are copied on the caller's thread, which is why they have
    /// a call of their own: most captures have none.
    pub fn capture_with_meta(
        &self,
        flow: FlowId,
        category: &'static str,
        meta: &[(&str, &str)],
        payload: impl Serialize + Send + 'static,
    ) {
        self.enqueue(
            flow,
            PendingBody::State {
                category,
                meta: meta
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
                payload: Box::new(payload),
            },
        );
    }

    /// Links `flow` to an id known outside Burgess, such as an order number,
    /// so an investigator can find the flow from that id.
    ///
    /// Takes no category: what is linked is the flow itself, not a part of
    /// the service's work.
    pub fn reference(&self, flow: FlowId, ref_type: &'static str, domain_id: impl Into<String>) {
        self.enqueue(
            flow,
            PendingBody::Reference {
                ref_type,
                domain_id: domain_id.into(),
            },
        );
    }

    /// Waits up to `timeout` for the records emitted so far to reach the
    /// sink and be delivered by it.
    ///
    /// The one call that waits. Make it when the service shuts down: records
    /// the sink still buffers when it is dropped are lost. See
    /// [`Sink::flush`].
    pub fn flush(&self, timeout: Duration) {
        if self.worker.is_none() {
            // Nothing would ever take the marker.
            return;
        }
        let deadline = Instant::now() + timeout;
        let (done, reached) = mpsc::sync_channel(1);
        let mut marker = Item::Flush { done };
        // A full queue has no room for the marker yet; the worker is
        // draining it, so wait for room.
        while let Err(rejected) = self.queue.push(marker) {
            if Instant::now() >= deadline {
                return;
            }
            marker = rejected;
            self.wake_worker();
            thread::sleep(Duration::from_millis(1));
        }
        self.wake_worker();
        let remaining = || deadline.saturating_duration_since(Instant::now());
        if reached.recv_timeout(remaining()).is_ok() {
            self.sink.flush(remaining());
        }
    }

    fn enqueue(&self, flow: FlowId, body: PendingBody) {
        self.queue.push_record(Pending {
            flow,
            time: self.clock.now().into(),
            body,
        });
    }

    /// Spares the worker the rest of its sleep. Never called on the paths
    /// that emit records, since waking a thread is a system call.
    fn wake_worker(&self) {
        if let Some(worker) = &self.worker {
            worker.thread().unpark();
        }
    }
}

impl Drop for Emitter {
    fn drop(&mut self) {
        self.queue.stop();
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            // A worker that panicked has nothing left to deliver.
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests;
