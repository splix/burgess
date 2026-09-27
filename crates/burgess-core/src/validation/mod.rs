//! Checks a decoded record must pass before it is stored.
//!
//! Decoding only proves a message is a well-formed record. These checks cover
//! what storage and lookups depend on: references that can be typed back into
//! a query, payloads that can be served back, and payloads small enough to keep
//! files and memory bounded. The writer runs them on every record; the producer
//! library can run the same checks to catch a bad record before it is sent,
//! since they need nothing but the [`Limits`].
//!
//! Service, category, the child subflow and record times have no rules yet
//! and are passed through as sent.

mod error;
mod limits;
mod record;
mod reference;
mod state;
mod typeable;

pub use error::ValidationError;
pub use limits::Limits;
pub use reference::RefField;

/// A value that can be checked, and normalised where the rules allow it,
/// before it is stored.
pub trait Validate {
    /// Checks the value, trimming references in place.
    ///
    /// Works in place so that a rejected value is still at hand to log or to
    /// hand back to the application. It may be partly trimmed by then.
    fn validate(&mut self, limits: &Limits) -> Result<(), ValidationError>;
}
