//! The states a lookup found, as the messages of a gRPC answer.

use std::num::NonZeroUsize;

use burgess_api::grpc::list_states_response::Item;
use burgess_api::grpc::{ListStatesResponse, Problem, Truncated};

use super::cut::Cut;
use super::row::state_row;
use crate::states_lookup::FoundStates;

impl FoundStates {
    /// The messages of the answer: the problems of the lookup, then the
    /// first `limit` states, then `truncated` if more were found.
    ///
    /// As in a tree answer, a payload problem comes just before the state
    /// that it is about.
    pub fn into_responses(self, limit: NonZeroUsize) -> Vec<ListStatesResponse> {
        let cut = Cut::new(&self.problems, self.rows, limit);
        let mut items: Vec<Item> = cut.problems.into_iter().map(problem).collect();
        for state in cut.rows {
            let (row, payload_problem) = state_row(state);
            items.extend(payload_problem.map(problem));
            items.push(Item::Row(row));
        }
        if cut.truncated {
            items.push(Item::Truncated(Truncated {}));
        }
        items
            .into_iter()
            .map(|item| ListStatesResponse { item: Some(item) })
            .collect()
    }
}

fn problem(message: String) -> Item {
    Item::Problem(Problem { message })
}
