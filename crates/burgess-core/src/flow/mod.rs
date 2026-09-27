//! Identifiers of flows and of the trees they form.
//!
//! Every record carries a flow id, and every component has to read the same
//! 16 bytes the same way. The partition key, the sort order of stored files and
//! the ids an investigator types into the CLI all depend on this layout.
//!
//! A flow id is a [`TreeId`] followed by a [`Subflow`]. The tree id starts with
//! the time the root was minted, so all flows of a tree sit next to each other
//! when records are sorted by id, and the id alone tells roughly when the tree
//! started.

mod flow_id;
mod flow_or_tree;
mod hex;
mod subflow;
mod tree_id;

pub use flow_id::FlowId;
pub use flow_or_tree::FlowOrTree;
pub use hex::{IdForm, IdParseError};
pub use subflow::Subflow;
pub use tree_id::TreeId;
