//! The query service. It answers lookups over the files the writer stored.
//!
//! It reads the object store directly and keeps no database of its own.
//! Investigations are rare and the data expires after a short retention, so a
//! database would cost more to run than the queries it would speed up.

use std::process::ExitCode;

fn main() -> ExitCode {
    eprintln!("burgess-reader is not implemented yet");
    ExitCode::FAILURE
}
