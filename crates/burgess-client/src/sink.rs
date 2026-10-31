//! Where emitted messages go.

use std::sync::Arc;

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
}

/// Lets several emitters, or an emitter and a test, share one sink.
impl<S: Sink + ?Sized> Sink for Arc<S> {
    fn send(&self, message: Message) {
        (**self).send(message);
    }
}
