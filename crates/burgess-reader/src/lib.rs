//! The query service. It answers lookups over the files the writer stored.
//!
//! It reads the object store directly and keeps no database of its own.
//! Investigations are rare and the data expires after a short retention, so a
//! database would cost more to run than the queries it would speed up.

pub mod manifests;

pub mod states_file;

pub mod tree_lookup;

pub mod pages;

pub mod rest;

pub mod config;

pub mod app;

#[cfg(test)]
mod test_support;
