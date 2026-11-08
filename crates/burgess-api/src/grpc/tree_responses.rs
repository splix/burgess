//! A page of tree rows as the messages of a gRPC answer.

use burgess_core::report::error_chain;

use super::{GetFlowResponse, GetTreeResponse, Problem, TreeRow, Truncated};
use super::{get_flow_response, get_tree_response};
use crate::Page;

/// A message of an answer of tree rows. GetTree and GetFlow have different
/// response types with the same content. This trait lets one function make
/// the messages of both.
pub trait TreeResponse {
    /// A message with a row.
    fn row(row: TreeRow) -> Self;
    /// A message with a problem.
    fn problem(problem: Problem) -> Self;
    /// The last message, when the page was cut at its limit.
    fn truncated() -> Self;
}

impl Page {
    /// The messages of this page: problems first, because they are known
    /// before the rows, then one message for each row, then `truncated` if
    /// the page was cut.
    ///
    /// A row that protobuf cannot hold changes to a problem. The other rows
    /// are still sent.
    pub fn into_responses<R: TreeResponse>(self) -> impl Iterator<Item = R> {
        let problems = self.problems.into_iter();
        let rows = self.rows.into_iter().map(TreeRow::try_from);
        let truncated = self.truncated.then(R::truncated);
        problems
            .map(|message| R::problem(Problem { message }))
            .chain(rows.map(|row| match row {
                Ok(row) => R::row(row),
                Err(error) => R::problem(Problem {
                    message: error_chain(&error),
                }),
            }))
            .chain(truncated)
    }
}

impl TreeResponse for GetTreeResponse {
    fn row(row: TreeRow) -> Self {
        GetTreeResponse {
            item: Some(get_tree_response::Item::Row(row)),
        }
    }

    fn problem(problem: Problem) -> Self {
        GetTreeResponse {
            item: Some(get_tree_response::Item::Problem(problem)),
        }
    }

    fn truncated() -> Self {
        GetTreeResponse {
            item: Some(get_tree_response::Item::Truncated(Truncated {})),
        }
    }
}

impl TreeResponse for GetFlowResponse {
    fn row(row: TreeRow) -> Self {
        GetFlowResponse {
            item: Some(get_flow_response::Item::Row(row)),
        }
    }

    fn problem(problem: Problem) -> Self {
        GetFlowResponse {
            item: Some(get_flow_response::Item::Problem(problem)),
        }
    }

    fn truncated() -> Self {
        GetFlowResponse {
            item: Some(get_flow_response::Item::Truncated(Truncated {})),
        }
    }
}
