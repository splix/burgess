//! Settings an emitter starts with.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::thread;

use burgess_core::validation::Limits;

use super::Emitter;
use crate::clock::{Clock, SystemClock};
use crate::queue::Queue;
use crate::sink::Sink;
use crate::worker::Worker;

/// Records the queue holds by default before it drops new ones.
///
/// Enough to ride out a burst, or a sink stalled for a moment, at tens of
/// thousands of records a second. The queue is allocated in full when the
/// emitter starts, at under a hundred bytes a record, so about 6 MB.
const DEFAULT_CAPACITY: NonZeroUsize = NonZeroUsize::new(65_536).expect("not zero");

/// Settings for an [`Emitter`], applied when it starts.
pub struct EmitterBuilder {
    service: String,
    clock: Arc<dyn Clock>,
    limits: Limits,
    capacity: NonZeroUsize,
}

impl EmitterBuilder {
    pub(super) fn new(service: String) -> Self {
        EmitterBuilder {
            service,
            clock: Arc::new(SystemClock),
            limits: Limits::default(),
            capacity: DEFAULT_CAPACITY,
        }
    }

    /// Uses `clock` to stamp records and to mint root flows, instead of the
    /// host's clock.
    pub fn clock(self, clock: impl Clock + 'static) -> Self {
        EmitterBuilder {
            clock: Arc::new(clock),
            ..self
        }
    }

    /// Checks records against `limits` before sending them, instead of the
    /// default [`Limits`].
    ///
    /// Set them to what the writer is configured with, so that a record is
    /// dropped here exactly when the writer would skip it.
    pub fn limits(self, limits: Limits) -> Self {
        EmitterBuilder { limits, ..self }
    }

    /// How many records may wait for the background thread before new ones
    /// are dropped. 65,536 by default.
    pub fn capacity(self, capacity: NonZeroUsize) -> Self {
        EmitterBuilder { capacity, ..self }
    }

    /// Starts the background thread that delivers to `sink`.
    pub fn start(self, sink: impl Sink + 'static) -> Emitter {
        let queue = Arc::new(Queue::new(self.capacity));
        let sink: Arc<dyn Sink> = Arc::new(sink);
        let worker = Worker::new(
            queue.clone(),
            sink.clone(),
            self.service,
            self.limits,
            self.clock.clone(),
        );
        let spawned = thread::Builder::new()
            .name("burgess-emitter".into())
            .spawn(move || worker.run());
        let worker = match spawned {
            Ok(handle) => Some(handle),
            Err(error) => {
                // Failing here would fail the application's start over
                // capture, which must never happen.
                tracing::error!(%error, "cannot start the emitter thread; no records will be sent");
                None
            }
        };
        Emitter {
            queue,
            clock: self.clock,
            sink,
            worker,
        }
    }
}
