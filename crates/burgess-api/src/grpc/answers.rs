//! The messages of an answer, and the page they make.

use burgess_core::report::error_chain;

use super::row_conversion::RowConversionError;
use super::{ListRefsResponse, ListStatesResponse, list_refs_response, list_states_response};
use crate::{Page, RefRow, Row};

/// What one message of an answer holds, with its row in the JSON form.
#[derive(Debug)]
pub enum PageItem<R> {
    /// A row, or why it cannot be in the page.
    Row(Result<R, RowConversionError>),
    /// Data that the lookup could not read.
    Problem(String),
    /// The last message, when more rows exist than the limit.
    Truncated,
}

/// A message of an answer that a page can read. Each method has its own
/// response type. This trait lets one function make the page of each.
pub trait AnswerMessage {
    /// The row of the page, in the JSON form.
    type Row;
    /// The item of the message. `None` for a message kind that is newer
    /// than this version.
    fn into_page_item(self) -> Option<PageItem<Self::Row>>;
}

impl<R> Page<R> {
    /// The page that the messages of an answer make, as REST sends it.
    ///
    /// A row that the page cannot hold, or a message of an unknown kind,
    /// becomes a problem. The other rows are kept.
    pub fn from_responses<M: AnswerMessage<Row = R>>(
        responses: impl IntoIterator<Item = M>,
    ) -> Self {
        let mut page = Page {
            rows: Vec::new(),
            truncated: false,
            problems: Vec::new(),
        };
        for response in responses {
            match response.into_page_item() {
                Some(PageItem::Row(Ok(row))) => page.rows.push(row),
                Some(PageItem::Row(Err(error))) => page.problems.push(error_chain(&error)),
                Some(PageItem::Problem(message)) => page.problems.push(message),
                Some(PageItem::Truncated) => page.truncated = true,
                None => page
                    .problems
                    .push("a message of an unknown kind was skipped".to_string()),
            }
        }
        page
    }
}

impl AnswerMessage for ListStatesResponse {
    type Row = Row;

    fn into_page_item(self) -> Option<PageItem<Row>> {
        Some(match self.item? {
            list_states_response::Item::Row(row) => PageItem::Row(Row::try_from(row)),
            list_states_response::Item::Problem(problem) => PageItem::Problem(problem.message),
            list_states_response::Item::Truncated(_) => PageItem::Truncated,
        })
    }
}

impl AnswerMessage for ListRefsResponse {
    type Row = RefRow;

    fn into_page_item(self) -> Option<PageItem<RefRow>> {
        Some(match self.item? {
            list_refs_response::Item::Row(row) => PageItem::Row(RefRow::try_from(row)),
            list_refs_response::Item::Problem(problem) => PageItem::Problem(problem.message),
            list_refs_response::Item::Truncated(_) => PageItem::Truncated,
        })
    }
}
