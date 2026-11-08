//! Running one command line.

use std::io::{self, Write};

use crate::args::{Cli, Command};
use crate::client::{ClientError, Lookup, QueryClient};
use crate::output::{Heading, write_notes};

/// How a lookup that answered went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Every row the service could find was read.
    Complete,
    /// The service could not read everything, so rows may be missing.
    Partial,
}

/// Why a command did not finish.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// No page came back.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// The rows could not be printed.
    #[error("printing failed")]
    Print(#[from] io::Error),
}

/// Runs `cli`, printing rows on `out` and notes on `err`.
pub async fn run(cli: Cli, out: &mut dyn Write, err: &mut dyn Write) -> Result<Outcome, RunError> {
    let (lookup, heading, page) = match cli.command {
        Command::Tree { id, page } => (Lookup::Tree(id.tree()), Heading::Tree(id.tree()), page),
        Command::Flow { id, page } => (Lookup::Flow(id), Heading::Flow(id), page),
    };
    let page = QueryClient::new(cli.server).page(lookup, page).await?;
    let printed = cli
        .output
        .write_rows(heading, &page, out)
        .and_then(|()| out.flush());
    match printed {
        // A reader such as `head` that has seen enough closes the pipe; the
        // rows it wanted were printed.
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {}
        printed => printed?,
    }
    write_notes(&page, err)?;
    Ok(if page.problems.is_empty() {
        Outcome::Complete
    } else {
        Outcome::Partial
    })
}

#[cfg(test)]
mod tests;
