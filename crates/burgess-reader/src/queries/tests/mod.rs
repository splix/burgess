//! Answers in pages: the pages together give the rows of one answer, and a
//! cursor that cannot continue is refused.

mod continuing;
mod paging;
mod problems;
mod retention;

use burgess_api::Page;
use burgess_api::grpc::AnswerMessage;
use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::RefRow;
use burgess_core::wire::UnixMillis;
use serde::Serialize;
use serde_json::Value;

use super::RequestError;

/// More pages than a test can need: a cursor that does not move on would
/// loop for ever.
const MOST_PAGES: usize = 1_000;

/// The rows of each page of an answer, from the first page to the page
/// without a cursor. `ask` sends the request with the cursor that it gets.
async fn pages<M>(
    ask: impl AsyncFnMut(Option<String>) -> Result<Vec<M>, RequestError>,
) -> Vec<Vec<Value>>
where
    M: AnswerMessage,
    M::Row: Serialize,
{
    each_page(ask)
        .await
        .into_iter()
        .map(|page| {
            assert!(page.problems.is_empty(), "{:?}", page.problems);
            json_rows(&page.rows)
        })
        .collect()
}

/// Each page of an answer, from the first page to the page without a
/// cursor. `ask` sends the request with the cursor that it gets.
async fn each_page<M: AnswerMessage>(
    mut ask: impl AsyncFnMut(Option<String>) -> Result<Vec<M>, RequestError>,
) -> Vec<Page<M::Row>> {
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let page = Page::from_responses(ask(cursor).await.expect("a page"));
        cursor = page.cursor.clone();
        pages.push(page);
        assert!(pages.len() < MOST_PAGES, "the cursors do not move on");
        if cursor.is_none() {
            return pages;
        }
    }
}

/// The rows of one answer that holds all of them.
async fn whole<M>(answer: Result<Vec<M>, RequestError>) -> Vec<Value>
where
    M: AnswerMessage,
    M::Row: Serialize,
{
    let page = Page::from_responses(answer.expect("an answer"));
    assert!(page.problems.is_empty(), "{:?}", page.problems);
    assert_eq!(page.cursor, None, "the answer must hold all rows");
    json_rows(&page.rows)
}

/// Rows as JSON, which compares all their fields.
fn json_rows<R: Serialize>(rows: &[R]) -> Vec<Value> {
    rows.iter()
        .map(|row| serde_json::to_value(row).unwrap())
        .collect()
}

/// A reference to `domain_id` by `subflow` of `tree` at `time`.
fn reference(domain_id: &str, tree: TreeId, subflow: u32, time: u64) -> RefRow {
    RefRow {
        ref_type: "order_id".into(),
        domain_id: domain_id.into(),
        service: "checkout".into(),
        category: None,
        tree,
        subflow: Subflow::from(subflow),
        time: UnixMillis::from(time),
    }
}
