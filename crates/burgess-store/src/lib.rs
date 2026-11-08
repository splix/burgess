//! Where Burgess keeps its files, as a deployment configures it.
//!
//! The writer stores files and the query service reads them, so both must
//! read the store settings the same way. One definition keeps a deployment's
//! `[store]` section valid for both.

mod config;
pub mod text;

pub use config::StoreConfig;
