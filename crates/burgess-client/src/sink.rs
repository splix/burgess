//! Where emitted messages go.

use std::sync::Arc;
use std::time::Duration;

use crate::message::Message;

/// Delivers messages to Burgess.
///
/// `send` returns nothing and must return quickly: the application calls it
/// on its own hot path, and capture is best effort. A sink that cannot deliver
/// a message logs that and drops it rather than blocking or failing the
/// caller.
pub trait Sink: Send + Sync {
    /// Hands over one message for delivery.
    fn send(&self, message: Message);

    /// Waits up to `timeout` for the messages handed over so far to be
    /// delivered.
    ///
    /// For shutdown: a sink that buffers messages loses whatever is still
    /// buffered when it is dropped. Returns after `timeout` at the latest,
    /// even if some messages were not delivered.
    fn flush(&self, _timeout: Duration) {
        // A sink that delivers on `send` has nothing left to wait for.
    }
}

/// Lets several emitters, or an emitter and a test, share one sink.
impl<S: Sink + ?Sized> Sink for Arc<S> {
    fn send(&self, message: Message) {
        (**self).send(message);
    }

    fn flush(&self, timeout: Duration) {
        (**self).flush(timeout);
    }
}
