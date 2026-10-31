//! The hand-over point between the callers' threads and the worker.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;

use crate::pending::{Item, Pending};

/// A bounded queue that callers push to without ever waiting.
///
/// Lock-free, so a caller is never stalled behind another thread, and the
/// worker is not woken on push, so pushing never makes a system call. A full
/// queue drops the record and counts it; the worker reports the count, since
/// logging is itself work the caller must not do.
pub(crate) struct Queue {
    items: ArrayQueue<Item>,
    overflowed: AtomicU64,
    stopping: AtomicBool,
}

impl Queue {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Queue {
            items: ArrayQueue::new(capacity.get()),
            overflowed: AtomicU64::new(0),
            stopping: AtomicBool::new(false),
        }
    }

    pub fn push_record(&self, pending: Pending) {
        if self.items.push(Item::Record(pending)).is_err() {
            self.overflowed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Queues `item` unless the queue is full, handing it back then.
    pub fn push(&self, item: Item) -> Result<(), Item> {
        self.items.push(item)
    }

    pub fn pop(&self) -> Option<Item> {
        self.items.pop()
    }

    /// Records dropped because the queue was full since the last call.
    pub fn take_overflowed(&self) -> u64 {
        self.overflowed.swap(0, Ordering::Relaxed)
    }

    /// Asks the worker to stop once it has drained the queue.
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }
}
