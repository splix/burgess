//! The command line.

use std::num::NonZeroU32;

use burgess_api::parse_time;
use burgess_core::flow::{FlowId, FlowOrTree};
use clap::{Args, Parser, Subcommand};
use jiff::Timestamp;

use crate::output::Output;
use crate::server::ServerAddress;

/// Where the query service is when nothing says otherwise: a service run
/// next to the investigator, as when trying Burgess out.
pub const DEFAULT_SERVER: &str = "http://localhost:8080";

/// Looks up State Capture traces in a Burgess query service.
#[derive(Debug, Parser)]
#[command(name = "burgess", version)]
pub struct Cli {
    /// The gRPC address of the query service: http:// or https://, a host
    /// and an optional port, without a path.
    #[arg(long, env = "BURGESS_SERVER", default_value = DEFAULT_SERVER, global = true)]
    pub server: ServerAddress,
    /// How rows are printed: a listing to read, or JSON lines, one row per
    /// line as the API returns it, to pipe into other tools.
    #[arg(long, short, value_enum, default_value_t = Output::Listing, global = true)]
    pub output: Output,
    /// What to look up.
    #[command(subcommand)]
    pub command: Command,
}

/// What to look up.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Every row of a tree. A flow id names its tree, so an id copied from
    /// any row of the tree works.
    Tree {
        /// A tree id, 24 hex characters, or a flow id, 32.
        #[arg(value_name = "ID")]
        id: FlowOrTree,
        /// Which rows to ask for.
        #[command(flatten)]
        page: PageArgs,
    },
    /// Every row of a flow and of the flows split off below it.
    Flow {
        /// A flow id, 32 hex characters.
        #[arg(value_name = "FLOW_ID")]
        id: FlowId,
        /// Which rows to ask for.
        #[command(flatten)]
        page: PageArgs,
    },
}

/// Which rows of a lookup to ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Args)]
pub struct PageArgs {
    /// The last day to look in, as an RFC 3339 time or unix milliseconds.
    /// Days are searched up to the day after it, since rows are filed by the
    /// day they were stored.
    #[arg(long, value_parser = parse_time)]
    pub until: Option<Timestamp>,
    /// The most rows to print. The service has a default and a maximum.
    #[arg(long)]
    pub limit: Option<NonZeroU32>,
}

#[cfg(test)]
mod tests;
