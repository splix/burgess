//! The references a lookup found, as the messages of a gRPC answer.

use std::num::NonZeroUsize;

use burgess_api::grpc::list_refs_response::Item;
use burgess_api::grpc::{ListRefsResponse, Problem, RefRow, Truncated};
use burgess_core::flow::FlowId;
use burgess_core::storage;

use super::cut::Cut;
use crate::refs_lookup::FoundRefs;

impl FoundRefs {
    /// The messages of the answer: the problems of the lookup, then the
    /// first `limit` references, then `truncated` if more were found.
    pub fn into_responses(self, limit: NonZeroUsize) -> Vec<ListRefsResponse> {
        let cut = Cut::new(&self.problems, self.rows, limit);
        let problems = cut
            .problems
            .into_iter()
            .map(|message| Item::Problem(Problem { message }));
        let rows = cut.rows.map(ref_row).map(Item::Row);
        problems
            .chain(rows)
            .chain(cut.truncated.then_some(Item::Truncated(Truncated {})))
            .map(|item| ListRefsResponse { item: Some(item) })
            .collect()
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
