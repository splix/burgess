//! `burgess-reader`, the query service. The `burgess_reader` library explains
//! how it answers lookups.
//!
//! Usage: `burgess-reader [--config <file.toml>]`. The API is served on the
//! `listen` address, gRPC and REST alike. Every setting may also
//! come from `BURGESS_READER__*` environment variables; logging follows
//! `RUST_LOG` (default `info`).
//!
//! Exit codes: 0 after a clean stop, 1 when the service stopped with an
//! error (a supervisor restarting it is the intended recovery), 2 for an
//! invalid command line, configuration or store, which a restart cannot
//! fix.

use std::path::PathBuf;
use std::process::ExitCode;

use burgess_core::report::error_chain;
use burgess_reader::app::Server;
use burgess_reader::config::Settings;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

const USAGE: &str = "usage: burgess-reader [--config <file.toml>]";

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let settings = match config_file()
        .and_then(|file| Settings::load(file.as_deref()).map_err(|error| error_chain(&error)))
    {
        Ok(settings) => settings,
        Err(problem) => {
            tracing::error!(%problem, "cannot start");
            return ExitCode::from(2);
        }
    };

    let server = match Server::bind(settings).await {
        Ok(server) => server,
        Err(error) => {
            tracing::error!(error = %error_chain(&error), "cannot start");
            return if error.restart_may_fix() {
                ExitCode::FAILURE
            } else {
                ExitCode::from(2)
            };
        }
    };

    let (stop, stopped) = watch::channel(false);
    tokio::spawn(async move {
        stop_signal().await;
        tracing::info!("stop requested; finishing the requests being answered");
        let _ = stop.send(true);
    });

    match server.run(stopped).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error_chain(&error), "service stopped with an error");
            ExitCode::FAILURE
        }
    }
}

/// The config file from `--config`, if given.
fn config_file() -> Result<Option<PathBuf>, String> {
    let mut args = std::env::args().skip(1);
    match (args.next(), args.next(), args.next()) {
        (None, _, _) => Ok(None),
        (Some(flag), Some(file), None) if flag == "--config" => Ok(Some(file.into())),
        _ => Err(USAGE.to_string()),
    }
}

/// Completes on Ctrl-C, or on SIGTERM where there is one, which is how
/// container runtimes and service managers ask a process to stop.
async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "cannot listen for SIGTERM; only Ctrl-C stops the service")
            }
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
