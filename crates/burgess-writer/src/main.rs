//! The single process that moves records from Kafka to object storage.
//!
//! Reads are rare and storage is cheap, so the writer does the work up front.
//! It batches and sorts the records, then writes Parquet files together with
//! small index objects. The index objects let the query service find a file
//! without listing the bucket.

use std::process::ExitCode;

fn main() -> ExitCode {
    eprintln!("burgess-writer is not implemented yet");
    ExitCode::FAILURE
}
