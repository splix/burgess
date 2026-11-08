//! The messages of an answer of tree rows.

use super::answers::{AnswerMessage, PageItem};
use super::{GetFlowResponse, GetTreeResponse, Problem, TreeRow, Truncated};
use super::{get_flow_response, get_tree_response};
use crate::Row;

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
pub trait TreeResponse: AnswerMessage<Row = Row> {
    /// The message that holds `item`.
    fn from_item(item: TreeItem) -> Self;
    /// The item of the message. `None` for a message kind that is newer
    /// than this version.
    fn into_item(self) -> Option<TreeItem>;
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

        impl AnswerMessage for $response {
            type Row = Row;

            fn into_page_item(self) -> Option<PageItem<Row>> {
                Some(match self.into_item()? {
                    TreeItem::Row(row) => PageItem::Row(Row::try_from(row)),
                    TreeItem::Problem(problem) => PageItem::Problem(problem.message),
                    TreeItem::Truncated => PageItem::Truncated,
                })
            }
        }
    };
}

tree_response!(GetTreeResponse, get_tree_response);
tree_response!(GetFlowResponse, get_flow_response);
