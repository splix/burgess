//! Publishes an application's internal state to Burgess.
//!
//! Applications link this library to mint flow ids and emit state, split and
//! reference records to Kafka. Capture is best effort. A failed publish must
//! never block or fail the application it observes, because the point of
//! capturing state is to debug that application, not to become a new way for it
//! to fail.
//!
//! An [`Emitter`] turns calls into records and hands them to a [`Sink`], which
//! delivers them. The emitter does that work on a thread of its own, so the
//! application's threads only queue records and never wait. Keeping delivery
//! behind a trait lets the same instrumentation publish to Kafka in
//! production and to a [`MemorySink`] in the application's own tests.

mod clock;
mod dropped;
mod emitter;
#[cfg(feature = "kafka")]
mod kafka;
mod memory_sink;
mod message;
mod pending;
mod queue;
mod sink;
mod throttle;
mod worker;

pub use clock::{Clock, SystemClock};
pub use emitter::{Emitter, EmitterBuilder};
#[cfg(feature = "kafka")]
pub use kafka::{Compression, KafkaConfig, KafkaSink, KafkaSinkError};
pub use memory_sink::MemorySink;
pub use message::Message;
pub use sink::Sink;

pub use burgess_core::flow::{FlowId, IdParseError, TreeId};
pub use burgess_core::validation::Limits;
