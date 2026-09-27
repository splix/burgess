//! The format of the records producers publish to Kafka.
//!
//! Producers ship inside applications and are upgraded on their own schedule,
//! so the writer reads messages from producers older and newer than itself. A
//! message it cannot read is skipped, not fatal, and [`DecodeError`] names the
//! reason so the skip can be logged and counted.

mod error;
mod marker;
mod record;
mod unix_millis;

pub use error::DecodeError;
pub use marker::MARKER;
pub use record::Record;
pub use unix_millis::UnixMillis;

/// Types generated from `proto/burgess/wire/v1/record.proto`.
///
/// Private so that other crates reach records only through [`Record`], which
/// checks what protobuf cannot: that the flow id has the right length and that
/// the record has a body.
pub(crate) mod proto {
    include!(concat!(env!("OUT_DIR"), "/burgess.wire.v1.rs"));
}

pub use proto::record::Body;
pub use proto::{PayloadType, Reference, Split, State};
