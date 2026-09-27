//! An id typed by an investigator, which may name one flow or a whole tree.

use std::fmt;
use std::str::FromStr;

use super::flow_id::FlowId;
use super::hex::{HexDigits, IdForm, IdParseError};
use super::tree_id::TreeId;

/// Either a single flow or a whole tree.
///
/// Lookups accept both, and the length of the text alone decides which one was
/// meant: 24 hex characters name a tree, 32 name a flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlowOrTree {
    /// Every flow of the tree.
    Tree(TreeId),
    /// One flow.
    Flow(FlowId),
}

impl FlowOrTree {
    /// The tree the id points into, which is enough to find its records.
    pub fn tree(&self) -> TreeId {
        match self {
            FlowOrTree::Tree(tree) => *tree,
            FlowOrTree::Flow(flow) => flow.tree(),
        }
    }
}

impl fmt::Display for FlowOrTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlowOrTree::Tree(tree) => tree.fmt(f),
            FlowOrTree::Flow(flow) => flow.fmt(f),
        }
    }
}

impl FromStr for FlowOrTree {
    type Err = IdParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = HexDigits::parse(text)?;
        if let Some(bytes) = digits.to_bytes() {
            Ok(FlowOrTree::Tree(TreeId::from(bytes)))
        } else if let Some(bytes) = digits.to_bytes() {
            Ok(FlowOrTree::Flow(FlowId::from(bytes)))
        } else {
            Err(digits.wrong_length(IdForm::TreeOrFlow))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLOW: &str = "000102030405060708090a0b0c0d0e0f";
    const TREE: &str = "000102030405060708090a0b";

    #[test]
    fn short_form_is_a_tree() {
        let id: FlowOrTree = TREE.parse().unwrap();
        assert_eq!(id, FlowOrTree::Tree(TREE.parse().unwrap()));
        assert_eq!(id.to_string(), TREE);
    }

    #[test]
    fn long_form_is_a_flow() {
        let id: FlowOrTree = FLOW.parse().unwrap();
        assert_eq!(id, FlowOrTree::Flow(FLOW.parse().unwrap()));
        assert_eq!(id.to_string(), FLOW);
    }

    #[test]
    fn both_forms_point_into_the_same_tree() {
        let tree = TREE.parse::<FlowOrTree>().unwrap().tree();
        let flow = FLOW.parse::<FlowOrTree>().unwrap().tree();
        assert_eq!(tree, flow);
    }

    #[test]
    fn rejects_other_lengths() {
        let error = FLOW[..28].parse::<FlowOrTree>().unwrap_err();
        assert_eq!(
            error,
            IdParseError::WrongLength {
                expected: IdForm::TreeOrFlow,
                found: 28
            }
        );
    }
}
