//! `burgess`, the command line client for the query service.
//!
//! It talks only to the query service, over REST. An investigator therefore
//! needs a server URL and nothing else: no Kafka access and no object store
//! credentials.

use std::process::ExitCode;

fn main() -> ExitCode {
    eprintln!("burgess is not implemented yet");
    ExitCode::FAILURE
}
