//! The references a lookup found, as the messages of a gRPC answer.

use std::num::NonZeroUsize;

use burgess_api::grpc::list_refs_response::Item;
use burgess_api::grpc::{ListRefsResponse, Problem, RefRow};
use burgess_core::flow::FlowId;
use burgess_core::storage;

use super::cut::Cut;
use crate::cursor::Cursors;
use crate::refs_lookup::FoundRefs;

impl FoundRefs {
    /// The messages of the answer: the problems of the lookup, then the
    /// first `limit` references, each with the cursor after it, as in a
    /// tree answer.
    pub fn into_responses(self, limit: NonZeroUsize, cursors: &Cursors) -> Vec<ListRefsResponse> {
        let cut = Cut::new(&self.problems, self.rows, limit, |found| {
            cursors.after(found.position, found.row.time)
        });
        let problems = cut.problems.into_iter().map(|message| ListRefsResponse {
            item: Some(Item::Problem(Problem { message })),
            cursor: None,
        });
        let rows = cut
            .rows
            .into_iter()
            .map(|(found, cursor)| ListRefsResponse {
                item: Some(Item::Row(ref_row(found.row))),
                cursor,
            });
        problems.chain(rows).collect()
    }
}

fn ref_row(row: storage::RefRow) -> RefRow {
    RefRow {
        flow_id: FlowId::new(row.tree, row.subflow).to_string(),
        tree: row.tree.to_string(),
        subflow: u32::from(row.subflow),
        // The writer stores times after year 9999 as its last instant, thus
        // all stored times fit.
        ts_ms: row.time.saturating_timestamp().as_millisecond(),
        ref_type: row.ref_type,
        domain_id: row.domain_id,
        service: row.service,
        category: row.category,
    }
}
