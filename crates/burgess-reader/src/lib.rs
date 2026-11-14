//! The query service. It answers lookups over the files the writer stored.
//!
//! It reads the object store directly and keeps no database of its own.
//! Investigations are rare and the data expires after a short retention, so a
//! database would cost more to run than the queries it would speed up.

pub mod manifests;

pub mod blooms;

pub mod parquet_file;

pub mod states_file;

pub mod filter;

pub mod ordering;

pub mod refs_file;

pub mod day_scan;

pub mod refs_lookup;

pub mod states_lookup;

pub mod tree_lookup;

pub mod pages;

pub mod lookup_time;

pub mod cursor;

pub mod queries;

pub mod grpc;

pub mod rest;

pub mod config;

pub mod app;

#[cfg(test)]
mod test_support;
