//! The pages of an answer report each file or manifest that cannot be read
//! one time, on the page whose rows it can hide.
//!
//! A page reads one row past its rows. A broken file between the last row
//! shown and that extra row hides only rows of the next pages, which read
//! it again.

use burgess_api::Page;
use burgess_api::grpc::get_tree_request::Tree;
use burgess_api::grpc::{GetTreeRequest, ListRefsRequest, ListStatesRequest, Order};
use burgess_core::storage::{Day, ObjectName};
use burgess_core::wire::UnixMillis;

use super::{each_page, json_rows, reference};
use crate::test_support::{Days, at, days_ago, flush, minted_on, noon, state_row};

/// Bytes that no reader can read as a file or a manifest.
fn broken() -> Vec<u8> {
    b"not a file".to_vec()
}

/// The problems of all `pages`, and their rows.
fn joined<R: serde::Serialize>(pages: &[Page<R>]) -> (Vec<String>, Vec<serde_json::Value>) {
    let problems = pages
        .iter()
        .flat_map(|page| page.problems.clone())
        .collect();
    let rows = pages
        .iter()
        .flat_map(|page| json_rows(&page.rows))
        .collect();
    (problems, rows)
}

/// The flush time of the flush `seq` of `day`, as the test days store it.
fn nnnn(day: Day, seq: u64) -> UnixMillis {
    UnixMillis::from(at(day, seq))
}

/// The walk reads newest flush first: the row of the first page, the
/// broken file, then the row of the next page.
#[tokio::test]
async fn the_pages_of_references_report_a_broken_file_one_time() {
    let day = days_ago(2);
    let tree = minted_on(day, 1);
    let days = Days::new();
    days.put_refs(
        day,
        &[
            &[reference("A", tree, 2, at(day, 2))],
            &[reference("A", tree, 1, at(day, 1))],
            &[reference("A", tree, 0, at(day, 0))],
        ],
    )
    .await;
    days.put_object(
        &ObjectName::RefsFile {
            day,
            nnnn: nnnn(day, 1),
        },
        broken(),
    )
    .await;
    let queries = days.queries();

    let pages = each_page(async |cursor| {
        queries
            .list_refs(ListRefsRequest {
                ref_type: "order_id".into(),
                domain_id: "A".into(),
                limit: Some(1),
                cursor,
                ..Default::default()
            })
            .await
    })
    .await;

    let (problems, rows) = joined(&pages);
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_eq!(rows.len(), 2, "{rows:?}");
}

/// The tree walks oldest day first: the row of the first page, the day
/// whose manifest is broken, then the row of the next page.
#[tokio::test]
async fn the_pages_of_a_tree_report_a_broken_day_one_time() {
    let (first, broken_day, last) = (days_ago(4), days_ago(3), days_ago(2));
    let tree = minted_on(first, 1);
    let days = Days::new();
    for day in [first, last] {
        days.put(
            day,
            &[&flush(vec![vec![state_row(tree, 0, at(day, 0), None)]])],
        )
        .await;
    }
    days.put_object(&ObjectName::Manifest { day: broken_day }, broken())
        .await;
    let queries = days.queries();

    let pages = each_page(async |cursor| {
        queries
            .get_tree(GetTreeRequest {
                tree: Some(Tree::TreeId(tree.to_string())),
                limit: Some(1),
                cursor,
                ..Default::default()
            })
            .await
    })
    .await;

    let (problems, rows) = joined(&pages);
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_eq!(rows.len(), 2, "{rows:?}");
}

/// In time order, the first page is full before the read of the later file
/// completes. Its problem is on the next page, which needs the file, and
/// not lost.
#[tokio::test]
async fn the_pages_of_states_by_time_report_a_broken_file_one_time() {
    let day = days_ago(2);
    let tree = minted_on(day, 1);
    let days = Days::new();
    days.put(
        day,
        &[
            &flush(vec![vec![
                state_row(tree, 0, at(day, 0), None),
                state_row(tree, 1, at(day, 1), None),
            ]]),
            &flush(vec![vec![state_row(tree, 2, at(day, 10), None)]]),
        ],
    )
    .await;
    days.put_object(
        &ObjectName::StatesFile {
            day,
            nnnn: nnnn(day, 1),
        },
        broken(),
    )
    .await;
    let queries = days.queries();

    let pages = each_page(async |cursor| {
        queries
            .list_states(ListStatesRequest {
                service: "checkout".into(),
                category: "step".into(),
                since_ms: Some(noon(days_ago(3)).as_millisecond()),
                until_ms: Some(noon(days_ago(1)).as_millisecond()),
                order: Order::TsAscending.into(),
                limit: Some(1),
                cursor,
                ..Default::default()
            })
            .await
    })
    .await;

    let (problems, rows) = joined(&pages);
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_eq!(rows.len(), 2, "{rows:?}");
}
