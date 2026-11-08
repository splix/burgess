//! The command line.

use std::num::NonZeroU32;

use burgess_api::parse_time;
use burgess_core::flow::{FlowId, FlowOrTree};
use clap::{Args, Parser, Subcommand, ValueEnum};
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
    /// The states of a service and category, in a time window, of any
    /// flow.
    States {
        /// The service that captured the states, as it named itself.
        service: String,
        /// The category of the states.
        category: String,
        /// Which states to ask for.
        #[command(flatten)]
        states: StatesArgs,
        /// The time window and the size of the page.
        #[command(flatten)]
        window: WindowArgs,
    },
    /// Each flow that recorded a reference to an outside id, such as an
    /// order id.
    Refs {
        /// The kind of the outside id, for example `order_id`.
        ref_type: String,
        /// The outside id. The comparison is exact and case-sensitive.
        domain_id: String,
        /// Only references that this service recorded.
        #[arg(long)]
        service: Option<String>,
        /// Only references recorded in this category.
        #[arg(long)]
        category: Option<String>,
        /// The time window and the size of the page.
        #[command(flatten)]
        window: WindowArgs,
    },
}

/// Which states of a service and category to ask for.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct StatesArgs {
    /// A filter on the payload, for example `amount > 100`,
    /// `tx.inputs[].value >= 1000` or `fee in 10..=20`.
    #[arg(long = "where", value_name = "EXPRESSION")]
    pub filter: Option<String>,
    /// A label that each state must have, with this value. Give it again for
    /// more labels; a state must have all of them.
    #[arg(long, value_name = "KEY=VALUE", value_parser = label)]
    pub meta: Vec<(String, String)>,
    /// The order of the states by their time. Without it, the states come
    /// in the order they are read, which is the fastest.
    #[arg(long, value_enum)]
    pub order: Option<TsOrder>,
}

/// The order of states by their time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TsOrder {
    /// Earliest first.
    Asc,
    /// Latest first.
    Desc,
}

/// The time window of a lookup, and how many rows to print.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Args)]
pub struct WindowArgs {
    /// The earliest time of a row, as an RFC 3339 time or unix milliseconds.
    /// For states, the service takes the 24 hours before `--until` when it
    /// is not given.
    #[arg(long, value_parser = parse_time)]
    pub since: Option<Timestamp>,
    /// The latest time of a row, as an RFC 3339 time or unix milliseconds.
    /// For states, the service takes now when it is not given; references
    /// then have no end.
    #[arg(long, value_parser = parse_time)]
    pub until: Option<Timestamp>,
    /// The most rows to print. The service has a default and a maximum.
    #[arg(long)]
    pub limit: Option<NonZeroU32>,
}

/// `KEY=VALUE` as a key and a value. The value can hold `=`.
///
/// The key and the value are trimmed: spaces around `=` are written to be
/// read, and the service compares labels exactly, thus it would find no
/// state with them.
fn label(text: &str) -> Result<(String, String), String> {
    let (key, value) = text
        .split_once('=')
        .map(|(key, value)| (key.trim(), value.trim()))
        .filter(|(key, _)| !key.is_empty())
        .ok_or_else(|| format!("expected KEY=VALUE, not {text:?}"))?;
    Ok((key.to_string(), value.to_string()))
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
