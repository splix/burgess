//! The states a lookup found, as the messages of a gRPC answer.

use std::num::NonZeroUsize;

use burgess_api::grpc::list_states_response::Item;
use burgess_api::grpc::{ListStatesResponse, Problem};

use super::cut::Cut;
use super::row::state_row;
use crate::cursor::Cursors;
use crate::states_lookup::FoundStates;

impl FoundStates {
    /// The messages of the answer: the problems of the lookup, then the
    /// first `limit` states, each with the cursor after it, as in a tree
    /// answer.
    ///
    /// As in a tree answer, a payload problem comes just before the state
    /// that it is about.
    pub fn into_responses(self, limit: NonZeroUsize, cursors: &Cursors) -> Vec<ListStatesResponse> {
        let cut = Cut::new(&self.problems, self.rows, limit, |state| {
            cursors.after(state.position, state.row.time)
        });
        let mut responses: Vec<ListStatesResponse> =
            cut.problems.into_iter().map(problem).collect();
        for (state, cursor) in cut.rows {
            let (row, payload_problem) = state_row(state.row);
            responses.extend(payload_problem.map(problem));
            responses.push(ListStatesResponse {
                item: Some(Item::Row(row)),
                cursor,
            });
        }
        responses
    }
}

fn problem(message: String) -> ListStatesResponse {
    ListStatesResponse {
        item: Some(Item::Problem(Problem { message })),
        cursor: None,
    }
}
