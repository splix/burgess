//! `burgess`, the command line client of the query service. The
//! `burgess_cli` library explains what it does.
//!
//! Usage: `burgess [--server <url>] [-o listing|jsonl] <command>`, where the
//! command is one of:
//!
//! - `tree|flow <id> [--until <time>]`
//! - `states <service> <category> [--where <expression>]
//!   [--meta <key=value>]... [--order asc|desc] [--since <time>]
//!   [--until <time>]`
//! - `refs <ref_type> <domain_id> [--service <name>] [--category <name>]
//!   [--since <time>] [--until <time>]`
//!
//! Each command also takes `[--limit <rows> | --all] [--page-size <rows>]
//! [--cursor <cursor>]`. The CLI asks for pages until it has the rows, and
//! prints them as one answer. When more rows exist, it prints the cursor
//! that continues after them.
//!
//! The server may also come from `BURGESS_SERVER`; logging follows
//! `RUST_LOG` (default `warn`) and goes to standard error.
//!
//! Exit codes: 0 when every row was read, 1 when the lookup failed, 2 for a
//! command line the CLI or the service refused, also for a cursor that
//! expired, 3 when rows were printed but the service could not read
//! everything, or a later page failed, so some may be missing.

use std::io;
use std::process::ExitCode;

use burgess_cli::args::Cli;
use burgess_cli::run::{Outcome, run};
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
            if let Some(pointer) = error.pointer() {
                eprintln!("{pointer}");
            }
            if error.is_usage() {
                ExitCode::from(USAGE)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}
