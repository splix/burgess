//! `burgess`, the command line client of the query service. The
//! `burgess_cli` library explains what it does.
//!
//! Usage: `burgess [--server <url>] [-o listing|jsonl] tree|flow <id>
//! [--until <time>] [--limit <rows>]`. The server may also come from
//! `BURGESS_SERVER`; logging follows `RUST_LOG` (default `warn`) and goes to
//! standard error.
//!
//! Exit codes: 0 when every row was read, 1 when the lookup failed, 2 for a
//! command line the CLI or the service refused, 3 when rows were printed but
//! the service could not read everything, so some may be missing.

use std::io;
use std::process::ExitCode;

use burgess_cli::args::Cli;
use burgess_cli::run::{Outcome, RunError, run};
use burgess_core::report::error_chain;
use clap::Parser;
use tracing_subscriber::EnvFilter;

/// Rows were printed, but some may be missing.
const PARTIAL: u8 = 3;
/// The command line was refused.
const USAGE: u8 = 2;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    match run(cli, &mut io::stdout().lock(), &mut io::stderr().lock()).await {
        Ok(Outcome::Complete) => ExitCode::SUCCESS,
        Ok(Outcome::Partial) => ExitCode::from(PARTIAL),
        Err(error) => {
            eprintln!("burgess: {}", error_chain(&error));
            match error {
                RunError::Client(error) if error.is_usage() => ExitCode::from(USAGE),
                _ => ExitCode::FAILURE,
            }
        }
    }
}
