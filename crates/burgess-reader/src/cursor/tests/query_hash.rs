use std::fmt::Debug;

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{
    GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest, Order,
};

use super::states_request;
use crate::cursor::{PagedRequest, QueryHash};

const TREE: &str = "0123456789abcdef01234567";
const FLOW: &str = "0123456789abcdef0123456700000001";

fn tree_request() -> GetTreeRequest {
    GetTreeRequest {
        tree: Some(Tree::TreeId(TREE.into())),
        ..GetTreeRequest::default()
    }
}

fn flow_request() -> GetFlowRequest {
    GetFlowRequest {
        flow_id: FLOW.into(),
        ..GetFlowRequest::default()
    }
}

fn refs_request() -> ListRefsRequest {
    ListRefsRequest {
        ref_type: "order_id".into(),
        domain_id: "A-1".into(),
        ..ListRefsRequest::default()
    }
}

/// Asserts that each of `others` has another hash than `first`.
fn each_differs<R: PagedRequest + Debug>(first: &R, others: &[R]) {
    for other in others {
        assert_ne!(QueryHash::of(first), QueryHash::of(other), "{other:?}");
    }
}

/// Asserts that each of `same` has the hash of `first`.
fn each_same<R: PagedRequest + Debug>(first: &R, same: &[R]) {
    for other in same {
        assert_eq!(QueryHash::of(first), QueryHash::of(other), "{other:?}");
    }
}

#[test]
fn each_field_of_a_states_request_is_in_the_hash() {
    let first = states_request();
    let mut other_meta = first.meta.clone();
    other_meta.insert("region".into(), "us".into());
    each_differs(
        &first,
        &[
            ListStatesRequest {
                service: "payment".into(),
                ..first.clone()
            },
            ListStatesRequest {
                category: "done".into(),
                ..first.clone()
            },
            ListStatesRequest {
                r#where: Some("amount > 101".into()),
                ..first.clone()
            },
            ListStatesRequest {
                r#where: None,
                ..first.clone()
            },
            ListStatesRequest {
                meta: other_meta,
                ..first.clone()
            },
            ListStatesRequest {
                meta: Default::default(),
                ..first.clone()
            },
            ListStatesRequest {
                since_ms: Some(1_794_563_600_000),
                ..first.clone()
            },
            ListStatesRequest {
                until_ms: Some(1_794_650_000_000),
                ..first.clone()
            },
            ListStatesRequest {
                order: Order::TsAscending.into(),
                ..first.clone()
            },
            ListStatesRequest {
                order: Order::Unspecified.into(),
                ..first.clone()
            },
        ],
    );
}

#[test]
fn each_field_of_a_refs_request_is_in_the_hash() {
    let first = refs_request();
    each_differs(
        &first,
        &[
            ListRefsRequest {
                ref_type: "invoice_id".into(),
                ..first.clone()
            },
            ListRefsRequest {
                domain_id: "a-1".into(),
                ..first.clone()
            },
            ListRefsRequest {
                service: Some("checkout".into()),
                ..first.clone()
            },
            ListRefsRequest {
                category: Some("step".into()),
                ..first.clone()
            },
            ListRefsRequest {
                since_ms: Some(0),
                ..first.clone()
            },
            ListRefsRequest {
                until_ms: Some(1_794_650_000_000),
                ..first.clone()
            },
        ],
    );
}

#[test]
fn each_field_of_a_tree_or_flow_request_is_in_the_hash() {
    let tree = tree_request();
    each_differs(
        &tree,
        &[
            GetTreeRequest {
                tree: Some(Tree::TreeId("0123456789abcdef01234568".into())),
                ..tree.clone()
            },
            // The same text in the other field of the oneof.
            GetTreeRequest {
                tree: Some(Tree::FlowId(TREE.into())),
                ..tree.clone()
            },
            GetTreeRequest {
                until_ms: Some(1_794_650_000_000),
                ..tree.clone()
            },
        ],
    );
    let flow = flow_request();
    each_differs(
        &flow,
        &[
            GetFlowRequest {
                flow_id: "0123456789abcdef0123456700000002".into(),
                ..flow.clone()
            },
            GetFlowRequest {
                until_ms: Some(1_794_650_000_000),
                ..flow.clone()
            },
        ],
    );
}

#[test]
fn the_same_fields_of_another_method_give_another_hash() {
    let tree = GetTreeRequest {
        tree: Some(Tree::TreeId(FLOW.into())),
        ..GetTreeRequest::default()
    };
    let states = ListStatesRequest {
        service: "a".into(),
        category: "b".into(),
        ..ListStatesRequest::default()
    };
    let refs = ListRefsRequest {
        ref_type: "a".into(),
        domain_id: "b".into(),
        ..ListRefsRequest::default()
    };

    assert_ne!(QueryHash::of(&tree), QueryHash::of(&flow_request()));
    assert_ne!(QueryHash::of(&states), QueryHash::of(&refs));
}

#[test]
fn page_options_are_not_in_the_hash() {
    let tree = tree_request();
    each_same(
        &tree,
        &[GetTreeRequest {
            limit: Some(7),
            cursor: Some("ANY".into()),
            ..tree.clone()
        }],
    );
    let refs = refs_request();
    each_same(
        &refs,
        &[ListRefsRequest {
            limit: Some(7),
            cursor: Some("ANY".into()),
            ..refs.clone()
        }],
    );
}

/// The service reads these values as no value, thus a client can send
/// either one on each page.
#[test]
fn values_that_the_service_reads_the_same_give_the_same_hash() {
    let states = ListStatesRequest {
        r#where: None,
        ..states_request()
    };
    each_same(
        &states,
        &[
            ListStatesRequest {
                r#where: Some(String::new()),
                ..states.clone()
            },
            ListStatesRequest {
                r#where: Some("  \t ".into()),
                ..states.clone()
            },
        ],
    );
    let refs = refs_request();
    each_same(
        &refs,
        &[
            ListRefsRequest {
                service: Some(String::new()),
                ..refs.clone()
            },
            ListRefsRequest {
                category: Some(String::new()),
                ..refs.clone()
            },
        ],
    );
}

/// A cursor that one reader gives must continue on each other reader, also
/// of another build. Thus the hash of each method must not change with the
/// build.
#[test]
fn the_hash_of_each_method_is_the_same_on_each_build() {
    let hashes = [
        QueryHash::of(&tree_request()),
        QueryHash::of(&flow_request()),
        QueryHash::of(&states_request()),
        QueryHash::of(&refs_request()),
    ]
    .map(u64::from);
    assert_eq!(
        hashes,
        [
            0xce8d_25ed_6a42_d6cb,
            0xc148_6f03_7ed0_3da9,
            0x4cdf_39f9_2d1b_3c20,
            0x6335_ce98_7918_ac25,
        ]
    );
}
