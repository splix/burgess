//! The messages of an answer of tree rows, and the page they make.

use burgess_core::report::error_chain;

use super::{GetFlowResponse, GetTreeResponse, Problem, TreeRow, Truncated};
use super::{get_flow_response, get_tree_response};
use crate::{Page, Row};

/// What one message of an answer of tree rows holds.
#[derive(Debug, Clone, PartialEq)]
pub enum TreeItem {
    /// A row of the tree.
    Row(TreeRow),
    /// Data that the lookup could not read.
    Problem(Problem),
    /// The last message, when more rows exist than the limit.
    Truncated,
}

/// A message of an answer of tree rows. GetTree and GetFlow have different
/// response types with the same content. This trait lets one function make
/// and read the messages of the two.
pub trait TreeResponse {
    /// The message that holds `item`.
    fn from_item(item: TreeItem) -> Self;
    /// The item of the message. `None` for a message kind that is newer
    /// than this version.
    fn into_item(self) -> Option<TreeItem>;
}

impl Page {
    /// The page that the messages of an answer make, as REST sends it.
    ///
    /// A row that the page cannot hold, or a message of an unknown kind,
    /// becomes a problem. The other rows are kept.
    pub fn from_responses<R: TreeResponse>(responses: impl IntoIterator<Item = R>) -> Self {
        let mut page = Page {
            rows: Vec::new(),
            truncated: false,
            problems: Vec::new(),
        };
        for response in responses {
            match response.into_item() {
                Some(TreeItem::Row(row)) => match Row::try_from(row) {
                    Ok(row) => page.rows.push(row),
                    Err(error) => page.problems.push(error_chain(&error)),
                },
                Some(TreeItem::Problem(problem)) => page.problems.push(problem.message),
                Some(TreeItem::Truncated) => page.truncated = true,
                None => page
                    .problems
                    .push("a message of an unknown kind was skipped".to_string()),
            }
        }
        page
    }
}

/// GetTree and GetFlow responses hold the same items, under different
/// generated types.
macro_rules! tree_response {
    ($response:ty, $item:ident) => {
        impl TreeResponse for $response {
            fn from_item(item: TreeItem) -> Self {
                let item = match item {
                    TreeItem::Row(row) => $item::Item::Row(row),
                    TreeItem::Problem(problem) => $item::Item::Problem(problem),
                    TreeItem::Truncated => $item::Item::Truncated(Truncated {}),
                };
                Self { item: Some(item) }
            }

            fn into_item(self) -> Option<TreeItem> {
                Some(match self.item? {
                    $item::Item::Row(row) => TreeItem::Row(row),
                    $item::Item::Problem(problem) => TreeItem::Problem(problem),
                    $item::Item::Truncated(_) => TreeItem::Truncated,
                })
            }
        }
    };
}

tree_response!(GetTreeResponse, get_tree_response);
tree_response!(GetFlowResponse, get_flow_response);
