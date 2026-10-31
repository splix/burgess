//! The thread that turns queued records into messages.

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use burgess_core::validation::{Limits, Validate};
use burgess_core::wire::Record;

use crate::clock::Clock;
use crate::dropped::{DropReason, DroppedRecord};
use crate::pending::{Item, Pending};
use crate::queue::Queue;
use crate::sink::Sink;
use crate::throttle::{Throttle, Verdict};

/// How long the worker sleeps while records keep coming.
///
/// Callers never wake the worker, because waking a thread is a system call
/// on the caller's thread. So the worker checks on its own, and records reach
/// the sink up to this much later.
const BUSY_POLL: Duration = Duration::from_millis(5);

/// How long the worker sleeps once nothing has come for a while. Each empty
/// check doubles the sleep up to this, so an idle emitter costs ten wake-ups
/// a second instead of two hundred. The first record after a quiet spell
/// reaches the sink up to this much later.
const IDLE_POLL: Duration = Duration::from_millis(100);

/// Longest the worker keeps delivering once the emitter is dropped. Dropping
/// waits for the worker, so this bounds how long a shutdown can hang on a
/// full queue or a slow sink.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);

/// Does everything about a record the caller must not wait for: writes the
/// payload as JSON, validates, encodes, and hands the message to the sink.
pub(crate) struct Worker {
    queue: Arc<Queue>,
    sink: Arc<dyn Sink>,
    service: String,
    limits: Limits,
    clock: Arc<dyn Clock>,
    drop_throttle: Throttle,
    overflow_throttle: Throttle,
    /// Records lost to a full queue and not yet reported in a warning.
    overflow_unreported: u64,
}

impl Worker {
    pub fn new(
        queue: Arc<Queue>,
        sink: Arc<dyn Sink>,
        service: String,
        limits: Limits,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Worker {
            queue,
            sink,
            service,
            limits,
            clock,
            drop_throttle: Throttle::default(),
            overflow_throttle: Throttle::default(),
            overflow_unreported: 0,
        }
    }

    /// Runs until the queue is stopped, then delivers what is left.
    pub fn run(mut self) {
        let mut sleep = BUSY_POLL;
        while !self.queue.is_stopping() {
            sleep = if self.drain() {
                BUSY_POLL
            } else {
                (sleep * 2).min(IDLE_POLL)
            };
            thread::park_timeout(sleep);
        }
        self.shut_down();
    }

    /// Handles everything queued, and tells whether there was anything.
    fn drain(&mut self) -> bool {
        let mut handled = false;
        while let Some(item) = self.queue.pop() {
            handled = true;
            self.handle(item);
            // Checked as records are handled, not only once the queue is
            // empty: under sustained overload it never is.
            self.report_overflow();
        }
        self.report_overflow();
        handled
    }

    /// Delivers what was queued before the stop, for at most
    /// [`SHUTDOWN_DRAIN`], and discards the rest.
    fn shut_down(&mut self) {
        let deadline = Instant::now() + SHUTDOWN_DRAIN;
        while let Some(item) = self.queue.pop() {
            if Instant::now() >= deadline {
                let discarded = 1 + std::iter::from_fn(|| self.queue.pop()).count();
                tracing::warn!(
                    service = self.service,
                    discarded,
                    "discarding records still queued at shutdown"
                );
                break;
            }
            self.handle(item);
        }
        self.overflow_unreported += self.queue.take_overflowed();
        if self.overflow_unreported > 0 {
            self.warn_overflow();
        }
    }

    fn handle(&self, item: Item) {
        match item {
            Item::Record(pending) => self.send_guarded(pending),
            // The caller flushes the sink itself, so that a slow flush
            // never stops the worker from draining the queue.
            Item::Flush { done } => {
                // The caller may have stopped waiting; nothing to tell then.
                let _ = done.try_send(());
            }
        }
    }

    /// Sends one record, surviving a panic in the payload's `Serialize` or
    /// in the sink: a dead worker would silently stop all capture.
    fn send_guarded(&self, pending: Pending) {
        let flow = pending.flow;
        if panic::catch_unwind(AssertUnwindSafe(|| self.send(pending))).is_err() {
            match self.drop_throttle.admit(self.clock.now()) {
                Verdict::Warn { suppressed } => tracing::warn!(
                    service = self.service,
                    %flow,
                    suppressed,
                    "dropping a record: writing or sending it panicked"
                ),
                Verdict::Quiet => tracing::debug!(
                    service = self.service,
                    %flow,
                    "dropping a record: writing or sending it panicked"
                ),
            }
        }
    }

    fn send(&self, pending: Pending) {
        let Pending { flow, time, body } = pending;
        let dropped = DroppedRecord {
            service: &self.service,
            category: body.category(),
            kind: body.kind(),
            flow,
        };
        let mut record = match body.into_body(&self.service) {
            Ok(body) => Record {
                flow_id: flow,
                time,
                body,
            },
            Err(error) => return self.log_drop(&dropped, error.into()),
        };
        if let Err(error) = record.validate(&self.limits) {
            return self.log_drop(&dropped, error.into());
        }
        self.sink.send(record.into());
    }

    fn log_drop(&self, dropped: &DroppedRecord, reason: DropReason) {
        dropped.log(&reason, self.drop_throttle.admit(self.clock.now()));
    }

    /// Warns about records lost to a full queue, at most once a minute, with
    /// how many were lost since the previous warning.
    fn report_overflow(&mut self) {
        self.overflow_unreported += self.queue.take_overflowed();
        if self.overflow_unreported == 0 {
            return;
        }
        if let Verdict::Warn { .. } = self.overflow_throttle.admit(self.clock.now()) {
            self.warn_overflow();
        }
    }

    fn warn_overflow(&mut self) {
        tracing::warn!(
            service = self.service,
            lost = self.overflow_unreported,
            "dropping records: the queue to the sink is full"
        );
        self.overflow_unreported = 0;
    }
}
