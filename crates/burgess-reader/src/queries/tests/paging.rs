//! The pages of an answer hold the rows of the answer that holds them all,
//! in the same order, each row one time.

use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{
    GetFlowRequest, GetTreeRequest, ListRefsRequest, ListStatesRequest, Order,
};
use burgess_core::flow::{FlowId, Subflow, TreeId};

use super::{pages, reference, whole};
use crate::test_support::{Days, at, days_ago, flush, minted_on, noon, state_row};

/// Page sizes that cut the answer inside a file, at the edge of a file and
/// of a day.
const PAGE_SIZES: [u32; 3] = [1, 2, 3];

/// A page that holds each row of the tests.
const ALL: u32 = 10_000;

/// A tree written on three days. Its first day has two flushes, and a row
/// of another tree is in its first file. The root splits off 1 and later
/// 3, and 1 splits off 2.
async fn tree_days() -> (Days, TreeId) {
    let (first, second, third) = (days_ago(4), days_ago(3), days_ago(2));
    let tree = minted_on(first, 5);
    let other = minted_on(first, 9);
    let days = Days::new();
    days.put(
        first,
        &[
            &flush(vec![
                vec![
                    state_row(tree, 0, at(first, 0), None),
                    state_row(tree, 0, at(first, 1), Some(1)),
                    state_row(tree, 1, at(first, 2), None),
                ],
                vec![state_row(other, 0, at(first, 3), None)],
            ]),
            &flush(vec![vec![
                state_row(tree, 1, at(first, 4), Some(2)),
                state_row(tree, 2, at(first, 5), None),
            ]]),
        ],
    )
    .await;
    days.put(
        second,
        &[&flush(vec![vec![
            state_row(tree, 0, at(second, 0), Some(3)),
            state_row(tree, 1, at(second, 1), None),
            state_row(tree, 2, at(second, 2), None),
            state_row(tree, 3, at(second, 3), None),
        ]])],
    )
    .await;
    days.put(
        third,
        &[&flush(vec![vec![state_row(tree, 2, at(third, 0), None)]])],
    )
    .await;
    (days, tree)
}

#[tokio::test]
async fn the_pages_of_a_tree_hold_the_rows_of_the_tree() {
    let (days, tree) = tree_days().await;
    let queries = days.queries();
    let request = |limit: u32, cursor: Option<String>| GetTreeRequest {
        tree: Some(Tree::TreeId(tree.to_string())),
        limit: Some(limit),
        cursor,
        ..Default::default()
    };

    let all = whole(queries.get_tree(request(ALL, None)).await).await;

    assert_eq!(all.len(), 10);
    for size in PAGE_SIZES {
        let pages = pages(async |cursor| queries.get_tree(request(size, cursor)).await).await;
        assert_eq!(pages.concat(), all, "pages of {size}");
    }
}

/// A split before the cut of a page still tells which flows are below the
/// flow, thus the next pages have their rows.
#[tokio::test]
async fn the_pages_of_a_flow_hold_the_rows_of_the_flow_and_the_flows_below_it() {
    let (days, tree) = tree_days().await;
    let queries = days.queries();
    let request = |limit: u32, cursor: Option<String>| GetFlowRequest {
        flow_id: FlowId::new(tree, Subflow::from(1)).to_string(),
        limit: Some(limit),
        cursor,
        ..Default::default()
    };

    let all = whole(queries.get_flow(request(ALL, None)).await).await;

    assert_eq!(all.len(), 6);
    for size in PAGE_SIZES {
        let pages = pages(async |cursor| queries.get_flow(request(size, cursor)).await).await;
        assert_eq!(pages.concat(), all, "pages of {size}");
    }
}

#[tokio::test]
async fn the_pages_of_references_hold_each_reference() {
    let (fourth, third, second) = (days_ago(4), days_ago(3), days_ago(2));
    let tree = |random: u8| minted_on(fourth, random);
    let days = Days::new();
    days.put_refs(
        fourth,
        &[
            &[
                reference("A", tree(1), 0, at(fourth, 0)),
                reference("B", tree(1), 1, at(fourth, 1)),
                reference("A", tree(2), 0, at(fourth, 2)),
            ],
            &[reference("A", tree(3), 1, at(fourth, 3))],
        ],
    )
    .await;
    days.put_refs(
        third,
        &[&[
            reference("A", tree(4), 0, at(third, 0)),
            reference("A", tree(4), 1, at(third, 1)),
            reference("A", tree(5), 2, at(third, 2)),
        ]],
    )
    .await;
    days.put_refs(
        second,
        &[
            &[reference("B", tree(6), 0, at(second, 0))],
            &[reference("A", tree(6), 1, at(second, 1))],
        ],
    )
    .await;
    let queries = days.queries();
    let request = |limit: u32, cursor: Option<String>| ListRefsRequest {
        ref_type: "order_id".into(),
        domain_id: "A".into(),
        limit: Some(limit),
        cursor,
        ..Default::default()
    };

    let all = whole(queries.list_refs(request(ALL, None)).await).await;

    assert_eq!(all.len(), 7);
    for size in PAGE_SIZES {
        let pages = pages(async |cursor| queries.list_refs(request(size, cursor)).await).await;
        assert_eq!(pages.concat(), all, "pages of {size}");
    }
}

/// The states of a window, in each order.
fn states_request(order: Order, limit: u32, cursor: Option<String>) -> ListStatesRequest {
    ListStatesRequest {
        service: "checkout".into(),
        category: "step".into(),
        since_ms: Some(noon(days_ago(6)).as_millisecond()),
        until_ms: Some(noon(days_ago(1)).as_millisecond()),
        order: order.into(),
        limit: Some(limit),
        cursor,
        ..Default::default()
    }
}

#[tokio::test]
async fn the_pages_of_states_hold_each_state_in_each_order() {
    let (days, _) = tree_days().await;
    let queries = days.queries();

    for order in [Order::Unspecified, Order::TsAscending, Order::TsDescending] {
        let all = whole(queries.list_states(states_request(order, ALL, None)).await).await;

        assert_eq!(all.len(), 8, "{order:?}");
        for size in PAGE_SIZES {
            let pages = pages(async |cursor| {
                queries
                    .list_states(states_request(order, size, cursor))
                    .await
            })
            .await;
            assert_eq!(pages.concat(), all, "{order:?} pages of {size}");
        }
    }
}

/// States of the same time in two files, where the file with the earlier
/// flush has the later first time. A page cut between them must not skip
/// or repeat a state.
#[tokio::test]
async fn the_pages_of_states_of_the_same_time_hold_each_state() {
    let day = days_ago(2);
    let time = at(day, 100);
    let tree = |random: u8| minted_on(day, random);
    let days = Days::new();
    days.put(
        day,
        &[
            &flush(vec![vec![
                state_row(tree(1), 0, time, None),
                state_row(tree(2), 0, time, None),
                state_row(tree(3), 0, time + 1, None),
            ]]),
            &flush(vec![vec![
                state_row(tree(4), 0, time - 1, None),
                state_row(tree(5), 0, time, None),
                state_row(tree(6), 0, time, None),
            ]]),
        ],
    )
    .await;
    let queries = days.queries();

    for order in [Order::TsAscending, Order::TsDescending] {
        let all = whole(queries.list_states(states_request(order, ALL, None)).await).await;

        assert_eq!(all.len(), 6, "{order:?}");
        for size in PAGE_SIZES {
            let pages = pages(async |cursor| {
                queries
                    .list_states(states_request(order, size, cursor))
                    .await
            })
            .await;
            assert_eq!(pages.concat(), all, "{order:?} pages of {size}");
        }
    }
}
