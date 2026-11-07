//! Lookups over days of real files and manifests, checking both the rows
//! returned and which objects were requested.

use std::sync::Arc;

use burgess_core::flow::{FlowId, Subflow, TreeId};
use burgess_core::storage::{
    Day, DayManifest, FileStats, FlushEntry, ObjectName, StateRow, StatesBounds,
};
use burgess_core::wire::UnixMillis;
use burgess_writer::batch::SortedBatch;
use burgess_writer::parquet_files::{FileWriter, ParquetConfig};
use jiff::Timestamp;
use jiff::civil::Date;
use object_store::ObjectStoreExt;
use object_store::path::Path;

use super::*;
use crate::manifests::{ManifestCache, ManifestCacheConfig, ManifestReadError};
use crate::states_file::{StatesFileConfig, StatesFileProblem, StatesFileReader};
use crate::test_support::{RecordingStore, days_ago, state_row, subflows};

fn noon(day: Day) -> Timestamp {
    Date::from(day)
        .at(12, 0, 0, 0)
        .to_zoned(jiff::tz::TimeZone::UTC)
        .unwrap()
        .timestamp()
}

/// A tree minted at noon of `day`; `random` orders trees of the same day.
fn minted_on(day: Day, random: u8) -> TreeId {
    let millis = noon(day).as_millisecond().to_be_bytes();
    let mut id = [random; 12];
    id[..6].copy_from_slice(&millis[2..]);
    TreeId::from(id)
}

/// `offset` milliseconds past noon of `day`, in unix milliseconds.
fn at(day: Day, offset: u64) -> u64 {
    noon(day).as_millisecond() as u64 + offset
}

/// `count` rows of `tree` written on `day`, every third a split.
fn rows(tree: TreeId, day: Day, count: u64) -> Vec<StateRow> {
    (0..count)
        .map(|i| state_row(tree, i as u32 % 2, at(day, i), (i % 3 == 2).then_some(7)))
        .collect()
}

/// The rows of one flush, in the order its states file stores them.
fn flush(parts: Vec<Vec<StateRow>>) -> Vec<StateRow> {
    let mut rows: Vec<StateRow> = parts.into_iter().flatten().collect();
    rows.sort_by_key(StateRow::sort_key);
    rows
}

/// A store holding days of flushes, as the writer leaves them.
struct Days {
    store: Arc<RecordingStore>,
}

impl Days {
    fn new() -> Self {
        Days {
            store: Arc::new(RecordingStore::default()),
        }
    }

    /// Stores `flushes` as the files and manifest of `day`, oldest first.
    async fn put(&self, day: Day, flushes: &[&[StateRow]]) {
        let writer = FileWriter::new(ParquetConfig::default()).unwrap();
        let mut manifest = DayManifest::new(day, "writer-1".parse().unwrap());
        for (seq, states) in flushes.iter().enumerate() {
            let nnnn = UnixMillis::from(noon(day).as_millisecond() as u64 + seq as u64);
            let files = writer
                .write_batch(&SortedBatch {
                    states: states.to_vec(),
                    ..SortedBatch::default()
                })
                .unwrap();
            let name = ObjectName::StatesFile { day, nnnn };
            manifest.entries.push(FlushEntry {
                nnnn,
                seq: seq as u64,
                states_bounds: bounds(states),
                states: FileStats {
                    rows: files.states.rows as u64,
                    bytes: files.states.bytes.len() as u64,
                },
                refs: FileStats {
                    rows: files.refs.rows as u64,
                    bytes: files.refs.bytes.len() as u64,
                },
            });
            self.put_object(&name, files.states.bytes).await;
        }
        let name = ObjectName::Manifest { day };
        self.put_object(&name, manifest.encode().unwrap()).await;
    }

    async fn put_object(&self, name: &ObjectName, bytes: Vec<u8>) {
        self.store
            .inner
            .put(&Path::from(name.to_string()), bytes.into())
            .await
            .unwrap();
    }

    fn lookup(&self, config: TreeLookupConfig) -> TreeLookup {
        let manifests = ManifestCache::new(self.store.clone(), ManifestCacheConfig::default());
        let files = StatesFileReader::new(self.store.clone(), StatesFileConfig::default());
        TreeLookup::new(Arc::new(manifests), files, config)
    }

    /// The rows found with the default config, which must have found no
    /// problems.
    async fn tree(&self, tree: TreeId, until: Option<Timestamp>) -> Vec<StateRow> {
        self.found(TreeLookupConfig::default(), tree, until).await
    }

    async fn found(
        &self,
        config: TreeLookupConfig,
        tree: TreeId,
        until: Option<Timestamp>,
    ) -> Vec<StateRow> {
        let found = self.lookup(config).tree(tree, until).await;
        assert!(found.problems.is_empty(), "{:?}", found.problems);
        found.rows
    }

    /// Stores a flush of trees minted on later days, so that `day` has
    /// files and none can hold a tree minted on an earlier day.
    async fn put_negative(&self, day: Day) {
        self.put(
            day,
            &[&flush(vec![rows(minted_on(days_ago(-1), 1), day, 2)])],
        )
        .await;
    }

    /// The days whose manifest was requested, in request order.
    fn manifest_days(&self) -> Vec<Day> {
        self.store
            .paths()
            .iter()
            .filter(|path| path.ends_with("_manifest"))
            .map(|path| path[..8].parse().unwrap())
            .collect()
    }

    /// The objects requested under `day`, one entry per request.
    fn requests_on(&self, day: Day) -> Vec<String> {
        let prefix = format!("{day}/");
        self.store
            .paths()
            .into_iter()
            .filter(|path| path.starts_with(&prefix))
            .collect()
    }
}

fn bounds(rows: &[StateRow]) -> Option<StatesBounds> {
    Some(StatesBounds {
        min_tree: rows.iter().map(|row| row.tree).min()?,
        max_tree: rows.iter().map(|row| row.tree).max()?,
        min_ts: rows.iter().map(|row| row.time).min()?,
        max_ts: rows.iter().map(|row| row.time).max()?,
    })
}

fn of(tree: TreeId, flushes: &[&Vec<StateRow>]) -> Vec<StateRow> {
    flushes
        .iter()
        .flat_map(|rows| rows.iter())
        .filter(|row| row.tree == tree)
        .cloned()
        .collect()
}

fn days(range: std::ops::RangeInclusive<i64>) -> Vec<Day> {
    range.rev().map(days_ago).collect()
}

#[tokio::test]
async fn returns_a_tree_spread_over_two_days_in_walk_order() {
    let (first, second) = (days_ago(4), days_ago(3));
    let tree = minted_on(first, 5);
    let neighbour = minted_on(first, 9);
    let early = flush(vec![rows(tree, first, 6), rows(neighbour, first, 4)]);
    let late = flush(vec![rows(tree, first, 3)]);
    let next_day = flush(vec![rows(tree, second, 5)]);
    let days = Days::new();
    days.put(first, &[&early, &late]).await;
    days.put(second, &[&next_day]).await;
    days.put_negative(days_ago(2)).await;
    days.put_negative(days_ago(1)).await;

    let found = days.tree(tree, None).await;

    assert_eq!(found, of(tree, &[&early, &late, &next_day]));
    assert_eq!(days.manifest_days(), self::days(1..=4));
}

#[tokio::test]
async fn reads_more_files_of_a_day_than_it_reads_at_once() {
    let day = days_ago(3);
    let tree = minted_on(day, 5);
    let flushes: Vec<Vec<StateRow>> = (0..6).map(|_| flush(vec![rows(tree, day, 3)])).collect();
    let refs: Vec<&[StateRow]> = flushes.iter().map(Vec::as_slice).collect();
    let days = Days::new();
    days.put(day, &refs).await;

    let found = days.tree(tree, None).await;

    let all: Vec<&Vec<StateRow>> = flushes.iter().collect();
    assert_eq!(found, of(tree, &all));
    assert!(flushes.len() > TreeLookupConfig::default().concurrent_files.get());
}

#[tokio::test]
async fn a_day_whose_bounds_exclude_the_tree_costs_its_manifest_only() {
    let (first, second) = (days_ago(4), days_ago(3));
    let tree = minted_on(first, 5);
    let days = Days::new();
    days.put(first, &[&flush(vec![rows(tree, first, 3)])]).await;
    days.put_negative(second).await;

    days.tree(tree, None).await;

    assert_eq!(days.requests_on(second), [format!("{second}/_manifest")]);
}

#[tokio::test]
async fn stops_after_one_negative_day_of_slack() {
    let tree = minted_on(days_ago(6), 5);
    let before = flush(vec![rows(tree, days_ago(6), 3)]);
    let days = Days::new();
    days.put(days_ago(6), &[&before]).await;
    days.put_negative(days_ago(5)).await;
    days.put_negative(days_ago(4)).await;
    days.put(days_ago(3), &[&flush(vec![rows(tree, days_ago(3), 3)])])
        .await;

    assert_eq!(days.tree(tree, None).await, of(tree, &[&before]));
    assert_eq!(days.manifest_days(), self::days(4..=6));
}

#[tokio::test]
async fn a_single_negative_day_does_not_end_the_walk() {
    let tree = minted_on(days_ago(6), 5);
    let before = flush(vec![rows(tree, days_ago(6), 3)]);
    let after = flush(vec![rows(tree, days_ago(4), 3)]);
    let days = Days::new();
    days.put(days_ago(6), &[&before]).await;
    days.put_negative(days_ago(5)).await;
    days.put(days_ago(4), &[&after]).await;
    days.put_negative(days_ago(3)).await;
    days.put_negative(days_ago(2)).await;

    assert_eq!(days.tree(tree, None).await, of(tree, &[&before, &after]));
    assert_eq!(days.manifest_days(), self::days(2..=6));
}

#[tokio::test]
async fn a_day_whose_bounds_admit_the_tree_is_positive_without_its_rows() {
    let tree = minted_on(days_ago(6), 5);
    let before = flush(vec![rows(tree, days_ago(6), 3)]);
    let around = flush(vec![
        rows(minted_on(days_ago(7), 5), days_ago(5), 2),
        rows(minted_on(days_ago(5), 5), days_ago(5), 2),
    ]);
    let after = flush(vec![rows(tree, days_ago(3), 3)]);
    let days = Days::new();
    days.put(days_ago(6), &[&before]).await;
    days.put(days_ago(5), &[&around]).await;
    days.put_negative(days_ago(4)).await;
    days.put(days_ago(3), &[&after]).await;
    days.put_negative(days_ago(2)).await;
    days.put_negative(days_ago(1)).await;

    assert_eq!(days.tree(tree, None).await, of(tree, &[&before, &after]));
}

#[tokio::test]
async fn a_day_whose_manifest_lists_no_files_does_not_end_the_walk() {
    // The writer empties a day's manifest when it drops the only flush it
    // could not finish before a restart.
    let tree = minted_on(days_ago(5), 5);
    let before = flush(vec![rows(tree, days_ago(5), 3)]);
    let after = flush(vec![rows(tree, days_ago(2), 3)]);
    let days = Days::new();
    days.put(days_ago(5), &[&before]).await;
    days.put_negative(days_ago(4)).await;
    days.put(days_ago(3), &[]).await;
    days.put(days_ago(2), &[&after]).await;

    assert_eq!(days.tree(tree, None).await, of(tree, &[&before, &after]));
}

#[tokio::test]
async fn days_without_a_manifest_do_not_end_the_walk() {
    // The writer was down for three days and stored the backlog afterwards.
    let tree = minted_on(days_ago(6), 5);
    let before = flush(vec![rows(tree, days_ago(6), 3)]);
    let after = flush(vec![rows(tree, days_ago(2), 3)]);
    let days = Days::new();
    days.put(days_ago(6), &[&before]).await;
    days.put(days_ago(2), &[&after]).await;
    days.put_negative(days_ago(1)).await;
    days.put_negative(days_ago(0)).await;

    assert_eq!(days.tree(tree, None).await, of(tree, &[&before, &after]));
    assert_eq!(days.manifest_days(), self::days(0..=6));
}

#[tokio::test]
async fn until_walks_past_negative_days() {
    let tree = minted_on(days_ago(6), 5);
    let before = flush(vec![rows(tree, days_ago(6), 3)]);
    let after = flush(vec![rows(tree, days_ago(2), 3)]);
    let days = Days::new();
    days.put(days_ago(6), &[&before]).await;
    for day in 3..=5 {
        days.put_negative(days_ago(day)).await;
    }
    days.put(days_ago(2), &[&after]).await;

    let found = days.tree(tree, Some(noon(days_ago(2)))).await;

    assert_eq!(found, of(tree, &[&before, &after]));
    assert_eq!(days.manifest_days(), self::days(1..=6));
}

#[tokio::test]
async fn until_reads_the_day_after_it_but_no_further() {
    // Rows written before `until` may be flushed after midnight.
    let tree = minted_on(days_ago(6), 5);
    let on_the_day = flush(vec![rows(tree, days_ago(6), 3)]);
    let flushed_after_midnight = flush(vec![rows(tree, days_ago(5), 3)]);
    let days = Days::new();
    days.put(days_ago(6), &[&on_the_day]).await;
    days.put(days_ago(5), &[&flushed_after_midnight]).await;
    days.put(days_ago(4), &[&flush(vec![rows(tree, days_ago(4), 3)])])
        .await;

    let found = days.tree(tree, Some(noon(days_ago(6)))).await;

    assert_eq!(found, of(tree, &[&on_the_day, &flushed_after_midnight]));
    assert_eq!(days.manifest_days(), self::days(5..=6));
}

#[tokio::test]
async fn until_two_days_before_the_tree_started_requests_nothing() {
    let tree = minted_on(days_ago(6), 5);
    let days = Days::new();
    days.put(days_ago(6), &[&flush(vec![rows(tree, days_ago(6), 3)])])
        .await;

    assert_eq!(days.tree(tree, Some(noon(days_ago(8)))).await, []);
    assert!(days.store.paths().is_empty());
}

#[tokio::test]
async fn until_in_the_future_stops_at_tomorrow() {
    let tree = minted_on(days_ago(2), 5);
    let days = Days::new();
    days.put(days_ago(2), &[&flush(vec![rows(tree, days_ago(2), 3)])])
        .await;

    days.tree(tree, Some(Timestamp::MAX)).await;

    assert_eq!(days.manifest_days(), self::days(-1..=2));
}

#[tokio::test]
async fn days_before_finds_rows_filed_before_the_ids_day() {
    // The producer clock ran ahead across midnight: the id is dated the day
    // after the flush that stored it.
    let tree = minted_on(days_ago(3), 5);
    let early = flush(vec![rows(tree, days_ago(4), 3)]);
    let days = Days::new();
    days.put(days_ago(4), &[&early]).await;
    days.put_negative(days_ago(3)).await;
    days.put_negative(days_ago(2)).await;

    assert_eq!(days.tree(tree, None).await, []);
    let config = TreeLookupConfig {
        days_before: 1,
        ..TreeLookupConfig::default()
    };
    assert_eq!(days.found(config, tree, None).await, of(tree, &[&early]));
}

#[tokio::test]
async fn days_before_further_back_than_any_day_starts_at_retention() {
    let oldest = days_ago(i64::from(
        ManifestCacheConfig::default().retention_days.get(),
    ));
    let tree = minted_on(days_ago(3), 5);
    let early = flush(vec![rows(tree, days_ago(5), 3)]);
    let days = Days::new();
    days.put(days_ago(5), &[&early]).await;

    let config = TreeLookupConfig {
        days_before: u32::MAX,
        ..TreeLookupConfig::default()
    };
    assert_eq!(days.found(config, tree, None).await, of(tree, &[&early]));
    assert_eq!(days.manifest_days().first(), Some(&oldest));
}

#[tokio::test]
async fn days_before_the_ids_day_do_not_count_towards_the_stop() {
    let tree = minted_on(days_ago(3), 5);
    let own = flush(vec![rows(tree, days_ago(3), 3)]);
    let days = Days::new();
    days.put_negative(days_ago(5)).await;
    days.put_negative(days_ago(4)).await;
    days.put(days_ago(3), &[&own]).await;

    let config = TreeLookupConfig {
        days_before: 2,
        ..TreeLookupConfig::default()
    };
    assert_eq!(days.found(config, tree, None).await, of(tree, &[&own]));
}

#[tokio::test]
async fn a_tree_minted_before_retention_is_found_in_retained_days() {
    let retained = days_ago(i64::from(
        ManifestCacheConfig::default().retention_days.get(),
    ));
    let tree = minted_on(days_ago(40), 5);
    let late = flush(vec![rows(tree, retained.next().unwrap(), 3)]);
    let days = Days::new();
    days.put(retained.next().unwrap(), &[&late]).await;

    assert_eq!(days.tree(tree, None).await, of(tree, &[&late]));
    assert_eq!(days.manifest_days().first(), Some(&retained));
}

#[tokio::test]
async fn an_unreadable_manifest_is_reported_and_the_walk_goes_on() {
    let tree = minted_on(days_ago(4), 5);
    let before = flush(vec![rows(tree, days_ago(4), 3)]);
    let after = flush(vec![rows(tree, days_ago(2), 3)]);
    let broken = days_ago(3);
    let days = Days::new();
    days.put(days_ago(4), &[&before]).await;
    days.put_object(
        &ObjectName::Manifest { day: broken },
        b"not a manifest".to_vec(),
    )
    .await;
    days.put(days_ago(2), &[&after]).await;

    let found = days
        .lookup(TreeLookupConfig::default())
        .tree(tree, None)
        .await;

    assert_eq!(found.rows, of(tree, &[&before, &after]));
    assert!(matches!(
        found.problems.as_slice(),
        [TreeLookupError::Manifest(ManifestReadError::Invalid { day, .. })] if *day == broken
    ));
}

#[tokio::test]
async fn a_listed_file_that_is_gone_is_reported_and_the_others_are_read() {
    let day = days_ago(3);
    let tree = minted_on(day, 5);
    let kept = flush(vec![rows(tree, day, 3)]);
    let days = Days::new();
    days.put(day, &[&flush(vec![rows(tree, day, 4)]), &kept])
        .await;
    let gone = ObjectName::StatesFile {
        day,
        nnnn: UnixMillis::from(noon(day).as_millisecond() as u64),
    };
    days.store
        .inner
        .delete(&Path::from(gone.to_string()))
        .await
        .unwrap();

    let found = days
        .lookup(TreeLookupConfig::default())
        .tree(tree, None)
        .await;

    assert_eq!(found.rows, of(tree, &[&kept]));
    let [TreeLookupError::File(error)] = found.problems.as_slice() else {
        panic!("unexpected problems: {:?}", found.problems);
    };
    assert_eq!(error.file(), &gone);
    assert!(matches!(error.problem(), StatesFileProblem::Store(_)));
}

#[tokio::test]
async fn a_flow_brings_the_flows_split_below_it_on_later_days() {
    let (first, second) = (days_ago(3), days_ago(2));
    let tree = minted_on(first, 5);
    let days = Days::new();
    days.put(
        first,
        &[&flush(vec![vec![
            state_row(tree, 0, at(first, 0), Some(1)),
            state_row(tree, 0, at(first, 1), Some(4)),
            state_row(tree, 1, at(first, 2), None),
            state_row(tree, 4, at(first, 3), None),
        ]])],
    )
    .await;
    days.put(
        second,
        &[&flush(vec![vec![
            state_row(tree, 1, at(second, 0), Some(2)),
            state_row(tree, 2, at(second, 1), Some(3)),
            state_row(tree, 3, at(second, 2), None),
        ]])],
    )
    .await;

    let found = days
        .lookup(TreeLookupConfig::default())
        .flow(FlowId::new(tree, Subflow::from(1)), None)
        .await;

    assert!(found.problems.is_empty(), "{:?}", found.problems);
    assert_eq!(subflows(&found.rows), [1, 1, 2, 3]);
}

#[tokio::test]
async fn a_flow_reports_what_the_tree_could_not_read() {
    let tree = minted_on(days_ago(3), 5);
    let broken = days_ago(2);
    let days = Days::new();
    days.put(
        days_ago(3),
        &[&flush(vec![vec![state_row(
            tree,
            1,
            at(days_ago(3), 0),
            None,
        )]])],
    )
    .await;
    days.put_object(
        &ObjectName::Manifest { day: broken },
        b"not a manifest".to_vec(),
    )
    .await;

    let found = days
        .lookup(TreeLookupConfig::default())
        .flow(FlowId::new(tree, Subflow::from(1)), None)
        .await;

    assert_eq!(subflows(&found.rows), [1]);
    assert!(matches!(
        found.problems.as_slice(),
        [TreeLookupError::Manifest(ManifestReadError::Invalid { day, .. })] if *day == broken
    ));
}

#[test]
fn a_lookup_can_run_on_any_thread() {
    fn assert_send<T: Send>(_: &T) {}
    let days = Days::new();
    let lookup = days.lookup(TreeLookupConfig::default());
    let future = lookup.tree(minted_on(days_ago(1), 5), None);
    assert_send(&future);
}
