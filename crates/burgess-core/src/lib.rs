//! Formats that every Burgess component must agree on.
//!
//! Producers, the writer and the query service run as separate processes and
//! are often deployed at different times. Keeping flow ids, the Kafka wire
//! format, record validation and the framed storage objects in one crate stops
//! them from drifting apart: changing a format means changing this crate, and
//! every component sees that change.

pub mod flow;

pub mod wire;

pub mod validation;
