//! Running one command line.

use std::io::{self, Write};
use std::num::NonZeroUsize;

use burgess_api::Page;
use unicode_width::UnicodeWidthChar;

use crate::args::{Cli, Command};
use crate::client::{ClientError, QueryClient};
use crate::output::{Heading, RefsHeading, StatesHeading, write_notes};
use crate::paging::{Paging, follow};
use crate::requests::{ArgsError, flow_request, refs_request, states_request, tree_request};

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
    /// The command line cannot make a request.
    #[error(transparent)]
    Args(#[from] ArgsError),
    /// The service refused the `where` filter, at `column`.
    #[error("the filter {expression:?} cannot be read")]
    Filter {
        /// The filter as given.
        expression: String,
        /// Where the service found the error, in characters from 1.
        column: NonZeroUsize,
        /// The refusal, in the words of the service.
        #[source]
        source: Box<ClientError>,
    },
    /// No page came back.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// The rows could not be printed.
    #[error("printing failed")]
    Print(#[from] io::Error),
}

impl RunError {
    /// Whether the command line is at fault, thus only a different command
    /// line can succeed.
    pub fn is_usage(&self) -> bool {
        match self {
            RunError::Args(_) | RunError::Filter { .. } => true,
            RunError::Client(error) => error.is_usage(),
            RunError::Print(_) => false,
        }
    }

    /// The line of the filter where the service found the error, and a
    /// mark under the character, when that is why the command failed.
    pub fn pointer(&self) -> Option<String> {
        let RunError::Filter {
            expression, column, ..
        } = self
        else {
            return None;
        };
        Some(pointer(expression, *column))
    }
}

/// The line of `expression` that holds `column`, and a mark under it.
///
/// A filter can span lines, since a line break is a space in it. The mark
/// is moved by the cells that a terminal gives each character before it,
/// thus it stays under the character also after a wide or a combining
/// character. A tab stays a tab, as the terminal moves it as the line.
fn pointer(expression: &str, column: NonZeroUsize) -> String {
    let chars: Vec<char> = expression.chars().collect();
    // The column after the last character is where an expression that
    // stops too soon fails.
    let at = (column.get() - 1).min(chars.len());
    let start = chars[..at]
        .iter()
        .rposition(|&c| c == '\n')
        .map_or(0, |line_break| line_break + 1);
    let end = chars[at..]
        .iter()
        .position(|&c| c == '\n' || c == '\r')
        .map_or(chars.len(), |line_break| at + line_break);
    let line: String = chars[start..end].iter().collect();
    let gap: String = chars[start..at]
        .iter()
        .map(|&c| match c {
            '\t' => "\t".to_string(),
            c => " ".repeat(c.width().unwrap_or(0)),
        })
        .collect();
    format!("  {line}\n  {gap}^")
}

/// Runs `cli`, printing rows on `out` and notes on `err`.
///
/// The request is made before the client, thus a command line that cannot
/// make one is refused whatever the server is.
pub async fn run(cli: Cli, out: &mut dyn Write, err: &mut dyn Write) -> Result<Outcome, RunError> {
    let output = cli.output;
    match cli.command {
        Command::Tree { id, days, paging } => {
            let request = tree_request(id.tree(), days);
            let client = QueryClient::new(cli.server)?;
            let paging = Paging::from(paging);
            let page = follow(request, &paging, async |page| client.tree(page).await).await?;
            let printed = output.write_rows(Heading::Tree(id.tree()), &page, out);
            finish(&page, printed, out, err)
        }
        Command::Flow { id, days, paging } => {
            let request = flow_request(id, days);
            let client = QueryClient::new(cli.server)?;
            let paging = Paging::from(paging);
            let page = follow(request, &paging, async |page| client.flow(page).await).await?;
            let printed = output.write_rows(Heading::Flow(id), &page, out);
            finish(&page, printed, out, err)
        }
        Command::States {
            service,
            category,
            states,
            window,
            paging,
        } => {
            let heading = StatesHeading {
                service: service.clone(),
                category: category.clone(),
            };
            let expression = states.filter.clone();
            let request = states_request(service, category, states, window)?;
            let client = QueryClient::new(cli.server)?;
            let paging = Paging::from(paging);
            let page = follow(request, &paging, async |page| client.states(page).await)
                .await
                .map_err(|error| refused_filter(error, expression))?;
            let printed = output.write_states(&heading, &page, out);
            finish(&page, printed, out, err)
        }
        Command::Refs {
            ref_type,
            domain_id,
            service,
            category,
            window,
            paging,
        } => {
            let heading = RefsHeading {
                ref_type: ref_type.clone(),
                domain_id: domain_id.clone(),
            };
            let request = refs_request(ref_type, domain_id, service, category, window);
            let client = QueryClient::new(cli.server)?;
            let paging = Paging::from(paging);
            let page = follow(request, &paging, async |page| client.refs(page).await).await?;
            let printed = output.write_refs(&heading, &page, out);
            finish(&page, printed, out, err)
        }
    }
}

/// `error` as a refused filter, when the service says where in
/// `expression` it found the error.
fn refused_filter(error: ClientError, expression: Option<String>) -> RunError {
    match (&error, expression) {
        (
            ClientError::Refused {
                where_column: Some(column),
                ..
            },
            Some(expression),
        ) => RunError::Filter {
            expression,
            column: *column,
            source: Box::new(error),
        },
        _ => RunError::Client(error),
    }
}

/// Ends a run whose rows `printed` tells how they were printed: the notes
/// on `err`, and how it went.
fn finish<R>(
    page: &Page<R>,
    printed: io::Result<()>,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Result<Outcome, RunError> {
    match printed.and_then(|()| out.flush()) {
        // A reader such as `head` that has seen enough closes the pipe; the
        // rows it wanted were printed.
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {}
        printed => printed?,
    }
    write_notes(page, err)?;
    Ok(if page.problems.is_empty() {
        Outcome::Complete
    } else {
        Outcome::Partial
    })
}

#[cfg(test)]
mod tests;
