//! A sink that keeps messages in memory.

use std::sync::{Mutex, PoisonError};

use crate::message::Message;
use crate::sink::Sink;

/// Keeps every message it is sent, in order, until taken.
///
/// Lets an application test its instrumentation, or run it without Kafka,
/// while seeing exactly the messages a Kafka sink would have produced.
#[derive(Debug, Default)]
pub struct MemorySink {
    messages: Mutex<Vec<Message>>,
}

impl MemorySink {
    /// Removes and returns the messages sent so far, oldest first.
    pub fn take(&self) -> Vec<Message> {
        std::mem::take(&mut *self.lock())
    }

    /// A thread that panicked while holding the lock left the list intact,
    /// since every change to it is a single push or take, so the poison is
    /// ignored rather than failing every later send.
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Message>> {
        self.messages.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Sink for MemorySink {
    fn send(&self, message: Message) {
        self.lock().push(message);
    }
}

#[cfg(test)]
mod tests {
    use burgess_core::wire::{Body, Record, Split, UnixMillis};

    use super::*;

    fn message(byte: u8) -> Message {
        Message::from(Record {
            flow_id: [byte; 16].into(),
            time: UnixMillis::from(1),
            body: Body::Split(Split::default()),
        })
    }

    #[test]
    fn take_returns_messages_in_order() {
        let sink = MemorySink::default();
        sink.send(message(1));
        sink.send(message(2));
        assert_eq!(sink.take(), vec![message(1), message(2)]);
    }

    #[test]
    fn take_empties_the_sink() {
        let sink = MemorySink::default();
        sink.send(message(1));
        sink.take();
        assert_eq!(sink.take(), vec![]);
    }
}
