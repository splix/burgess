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
}

/// A message of an answer that a page can read. Each method has its own
/// response type. This trait lets one function make the page of each.
pub trait AnswerMessage {
    /// The row of the page, in the JSON form.
    type Row;
    /// The cursor after the row of the message, if more rows come after it.
    fn cursor_after(&self) -> Option<&str>;
    /// Whether the message holds a row, thus a cursor or the end of the
    /// answer. A problem message has neither.
    fn is_row(&self) -> bool;
    /// The item of the message. `None` for a message kind that is newer
    /// than this version.
    fn into_page_item(self) -> Option<PageItem<Self::Row>>;
}

impl<R> Page<R> {
    /// The page that the messages of an answer make, as REST sends it.
    ///
    /// A row that the page cannot hold, or a message of an unknown kind,
    /// becomes a problem. The other rows are kept. The cursor of the page is
    /// the cursor of the last row message, also of a row that it cannot
    /// hold, thus a page has no cursor when the answer has no more rows. A
    /// problem message has no cursor, and a stream that breaks after one
    /// still continues after the row before it.
    pub fn from_responses<M: AnswerMessage<Row = R>>(
        responses: impl IntoIterator<Item = M>,
    ) -> Self {
        let mut page = Page {
            rows: Vec::new(),
            cursor: None,
            problems: Vec::new(),
        };
        for response in responses {
            if response.is_row() {
                page.cursor = response.cursor_after().map(str::to_string);
            }
            match response.into_page_item() {
                Some(PageItem::Row(Ok(row))) => page.rows.push(row),
                Some(PageItem::Row(Err(error))) => page.problems.push(error_chain(&error)),
                Some(PageItem::Problem(message)) => page.problems.push(message),
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

    fn cursor_after(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    fn is_row(&self) -> bool {
        matches!(self.item, Some(list_states_response::Item::Row(_)))
    }

    fn into_page_item(self) -> Option<PageItem<Row>> {
        Some(match self.item? {
            list_states_response::Item::Row(row) => PageItem::Row(Row::try_from(row)),
            list_states_response::Item::Problem(problem) => PageItem::Problem(problem.message),
        })
    }
}

impl AnswerMessage for ListRefsResponse {
    type Row = RefRow;

    fn cursor_after(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    fn is_row(&self) -> bool {
        matches!(self.item, Some(list_refs_response::Item::Row(_)))
    }

    fn into_page_item(self) -> Option<PageItem<RefRow>> {
        Some(match self.item? {
            list_refs_response::Item::Row(row) => PageItem::Row(RefRow::try_from(row)),
            list_refs_response::Item::Problem(problem) => PageItem::Problem(problem.message),
        })
    }
}
