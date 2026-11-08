//! The rows and problems of a lookup as the messages of an answer.

use std::num::NonZeroUsize;

use burgess_api::grpc::{Problem, TreeItem, TreeResponse};
use burgess_core::report::error_chain;

use super::row::tree_row;
use crate::tree_lookup::FoundTree;

impl FoundTree {
    /// The messages of the answer: the problems of the lookup, then the
    /// first `limit` rows, then `truncated` if more rows were found.
    ///
    /// A payload problem comes just before the row that it is about, thus a
    /// client can show the two together. Rows after the limit are not sent,
    /// thus their payloads are not checked.
    pub fn into_responses<R: TreeResponse>(self, limit: NonZeroUsize) -> Vec<R> {
        let truncated = self.rows.len() > limit.get();
        let mut responses: Vec<R> = self
            .problems
            .iter()
            .map(|problem| problem_response(error_chain(problem)))
            .collect();
        for row in self.rows.into_iter().take(limit.get()) {
            let (row, problem) = tree_row(row);
            responses.extend(problem.map(problem_response));
            responses.push(R::from_item(TreeItem::Row(row)));
        }
        if truncated {
            responses.push(R::from_item(TreeItem::Truncated));
        }
        responses
    }
}

fn problem_response<R: TreeResponse>(message: String) -> R {
    R::from_item(TreeItem::Problem(Problem { message }))
}
