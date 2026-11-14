//! Each row that a read gives has its number in the file, also when the
//! read skips pages and row groups. A cursor names the last row of a page
//! by this number, thus a read after it must start exactly there.

use jiff::Timestamp;

use super::*;
use crate::ordering::TsOrder;
use crate::states_file::{FileKey, ScanOrder};

/// A file of three trees over several row groups and many pages.
async fn fixture() -> Fixture {
    let fixture = Fixture::new(&[(tree(1), 500), (tree(2), 2500), (tree(3), 500)], 1000).await;
    assert!(fixture.metadata().num_row_groups() > 3);
    fixture
}

/// The states of `category` in the window of every row.
fn filter(category: &str) -> StatesFilter {
    StatesFilter {
        service: "checkout".into(),
        category: category.into(),
        meta: BTreeMap::new(),
        since: Timestamp::from_millisecond(FIRST_MS as i64).unwrap(),
        until: Timestamp::from_millisecond((FIRST_MS + 1_000_000) as i64).unwrap(),
        payload: None,
    }
}

/// The numbers of the rows of the file that are states of `category`.
fn states_of(fixture: &Fixture, category: &str) -> Vec<u64> {
    fixture
        .rows
        .iter()
        .zip(0..)
        .filter(|(row, _)| {
            row.category == category && matches!(row.entry, StateEntry::State { .. })
        })
        .map(|(_, number)| number)
        .collect()
}

fn numbers<T>(rows: &[Numbered<T>]) -> Vec<u64> {
    rows.iter().map(|row| row.number).collect()
}

impl Fixture {
    async fn tree_after(&self, tree: TreeId, after: Option<u64>) -> Vec<Numbered<StateRow>> {
        StatesFileReader::new(self.store.clone(), self.tail_only_config())
            .tree(&self.name, self.stats(), tree, after)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn the_rows_of_a_tree_have_their_numbers_in_the_file() {
    let fixture = fixture().await;
    let range = fixture.rows_of_tree(tree(2));

    let rows = fixture.tree_after(tree(2), None).await;

    let expected: Vec<u64> = (range.start as u64..range.end as u64).collect();
    assert_eq!(numbers(&rows), expected);
}

#[tokio::test]
async fn a_tree_read_after_a_row_starts_at_the_next_row() {
    let fixture = fixture().await;
    let range = fixture.rows_of_tree(tree(2));
    let after = range.start as u64 + 1_234;

    let rows = fixture.tree_after(tree(2), Some(after)).await;

    let expected: Vec<u64> = (after + 1..range.end as u64).collect();
    assert_eq!(numbers(&rows), expected);
    assert_eq!(
        rows.iter().map(|row| row.row.clone()).collect::<Vec<_>>(),
        fixture.rows[after as usize + 1..range.end as usize]
    );
}

#[tokio::test]
async fn the_states_of_a_scan_have_their_numbers_in_the_file() {
    let fixture = fixture().await;

    let states = fixture
        .scan_numbered(filter("step-3"), 10_000, ScanOrder::InFile { after: None })
        .await;

    assert_eq!(numbers(&states), states_of(&fixture, "step-3"));
}

/// The limit counts the states after the row, thus a page in the middle
/// of a file is as long as the first.
#[tokio::test]
async fn a_scan_after_a_row_gives_the_next_states_up_to_the_limit() {
    let fixture = fixture().await;
    let all = states_of(&fixture, "step-3");
    let after = all[200];

    let states = fixture
        .scan_numbered(
            filter("step-3"),
            5,
            ScanOrder::InFile { after: Some(after) },
        )
        .await;

    assert_eq!(numbers(&states), all[201..206]);
}

/// States of the same time sort by their number, thus a scan by time after
/// a key gives the states with a larger key, from the smallest.
#[tokio::test]
async fn a_scan_by_time_after_a_key_gives_the_next_keys() {
    let fixture = fixture().await;
    let order = TsOrder::Ascending;
    let key = |number: u64| FileKey {
        time: order.key(fixture.rows[number as usize].time),
        row: number,
    };
    let mut keys: Vec<FileKey> = states_of(&fixture, "step-3").into_iter().map(key).collect();
    keys.sort();
    let after = keys[300];

    let states = fixture
        .scan_numbered(
            filter("step-3"),
            5,
            ScanOrder::ByTime {
                order,
                after: Some(after),
            },
        )
        .await;

    let found: Vec<FileKey> = numbers(&states).into_iter().map(key).collect();
    assert_eq!(found, keys[301..306]);
}
