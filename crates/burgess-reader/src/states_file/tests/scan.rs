//! Scans of files written by the real writer: which states they keep, and
//! which bytes of the file they fetch.

use jiff::Timestamp;

use super::*;

fn ms(offset: u64) -> Timestamp {
    Timestamp::from_millisecond((FIRST_MS + offset) as i64).unwrap()
}

/// The states of `category` in the window of every row of [`rows_of`].
fn filter(category: &str) -> StatesFilter {
    StatesFilter {
        service: "checkout".into(),
        category: category.into(),
        meta: BTreeMap::new(),
        since: ms(0),
        until: ms(1_000_000),
    }
}

fn labels(labels: &[(&str, &str)]) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// `row` as a scan gives it, or `None` for a split.
fn stored(row: &StateRow) -> Option<StoredState> {
    let StateEntry::State {
        meta,
        payload_type,
        payload,
    } = &row.entry
    else {
        return None;
    };
    Some(StoredState {
        tree: row.tree,
        subflow: row.subflow,
        time: row.time,
        service: row.service.clone(),
        category: row.category.clone(),
        meta: meta.clone(),
        payload_type: *payload_type,
        payload: payload.clone(),
    })
}

impl Fixture {
    async fn scan_with(
        &self,
        config: FileReadConfig,
        filter: StatesFilter,
    ) -> Result<Vec<StoredState>, StatesReadError> {
        StatesFileReader::new(self.store.clone(), config)
            .states(
                &self.name,
                self.stats(),
                &Arc::new(filter),
                NonZeroUsize::MAX,
            )
            .await
    }

    async fn scan(&self, filter: StatesFilter) -> Vec<StoredState> {
        self.scan_with(self.tail_only_config(), filter)
            .await
            .unwrap()
    }

    async fn scan_up_to(&self, filter: StatesFilter, limit: usize) -> Vec<StoredState> {
        StatesFileReader::new(self.store.clone(), self.tail_only_config())
            .states(
                &self.name,
                self.stats(),
                &Arc::new(filter),
                NonZeroUsize::new(limit).unwrap(),
            )
            .await
            .unwrap()
    }

    /// The byte ranges of the column chunks of `column` and of the columns
    /// inside it.
    fn chunks_of(&self, column: StatesColumn) -> Vec<Range<u64>> {
        let metadata = self.metadata();
        let leaves: Vec<usize> = metadata
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, leaf)| leaf.path().parts()[0] == column.name())
            .map(|(leaf, _)| leaf)
            .collect();
        metadata
            .row_groups()
            .iter()
            .flat_map(|row_group| {
                leaves.iter().map(|&leaf| {
                    let (start, length) = row_group.column(leaf).byte_range();
                    start..start + length
                })
            })
            .collect()
    }

    /// The states that `keep` picks, in file order.
    fn states(&self, keep: impl Fn(&StateRow) -> bool) -> Vec<StoredState> {
        self.rows
            .iter()
            .filter(|row| keep(row))
            .filter_map(stored)
            .collect()
    }

    /// The leaf index of the payload column.
    fn payload_column(&self) -> usize {
        self.metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .position(|column| column.path().string() == StatesColumn::Payload.name())
            .unwrap()
    }
}

#[tokio::test]
async fn keeps_the_states_of_the_service_and_category_in_file_order() {
    let fixture = Fixture::new(&[(tree(1), 3000), (tree(2), 40), (tree(3), 3000)], 1 << 20).await;
    let expected = fixture.states(|row| row.category == "step-3");
    assert!(!expected.is_empty());

    assert_eq!(fixture.scan(filter("step-3")).await, expected);
}

#[tokio::test]
async fn splits_are_not_kept() {
    let fixture = Fixture::new(&[(tree(2), 100)], 1 << 20).await;
    let split = fixture
        .rows
        .iter()
        .find(|row| matches!(row.entry, StateEntry::Split { .. }))
        .unwrap();

    let states = fixture.scan(filter(&split.category)).await;

    assert_eq!(states, fixture.states(|row| row.category == split.category));
    assert!(states.iter().all(|state| state.time != split.time));
}

#[tokio::test]
async fn another_service_gives_no_states() {
    let fixture = Fixture::new(&[(tree(2), 100)], 1 << 20).await;

    let states = fixture
        .scan(StatesFilter {
            service: "shipping".into(),
            ..filter("step-3")
        })
        .await;

    assert_eq!(states, []);
}

#[tokio::test]
async fn the_window_includes_both_ends() {
    let fixture = Fixture::new(&[(tree(1), 100), (tree(2), 100)], 1 << 20).await;
    // In `rows_of`, row `i` is at `i` ms and in category `i % 7`, thus the
    // window has the rows 10 and 17 of each tree.
    let window = StatesFilter {
        since: ms(10),
        until: ms(17),
        ..filter("step-3")
    };

    let states = fixture.scan(window).await;

    let times: Vec<u64> = states
        .iter()
        .map(|state| u64::from(state.time) - FIRST_MS)
        .collect();
    assert_eq!(times, [10, 17, 10, 17]);
}

#[tokio::test]
async fn each_label_of_the_filter_must_be_on_the_state() {
    let fixture = Fixture::new(&[(tree(1), 100), (tree(2), 100)], 1 << 20).await;
    let with = |meta| StatesFilter {
        meta,
        ..filter("step-3")
    };

    let one = fixture.scan(with(labels(&[("n", "17")]))).await;
    let two = fixture.scan(with(labels(&[("n", "17"), ("x", "1")]))).await;
    let other_value = fixture.scan(with(labels(&[("n", "18")]))).await;

    assert_eq!(
        one,
        fixture.states(|row| row.time == UnixMillis::from(FIRST_MS + 17))
    );
    assert_eq!(two, []);
    assert_eq!(other_value, []);
}

#[tokio::test]
async fn no_payload_is_fetched_when_no_state_passes() {
    let fixture = Fixture::new(&[(tree(1), 3000), (tree(2), 3000)], 1 << 20).await;

    assert_eq!(fixture.scan(filter("step-9")).await, []);

    let payload = fixture.chunks_of(StatesColumn::Payload);
    assert!(!payload.is_empty());
    for chunk in payload {
        assert!(!fetched(&fixture.store, &chunk));
    }
}

#[tokio::test]
async fn labels_are_not_read_without_a_label_to_match() {
    let fixture = Fixture::new(&[(tree(1), 3000), (tree(2), 3000)], 1 << 20).await;

    assert_eq!(fixture.scan(filter("step-9")).await, []);

    let meta = fixture.chunks_of(StatesColumn::Meta);
    assert_eq!(meta.len(), 2, "keys and values");
    for chunk in meta {
        assert!(!fetched(&fixture.store, &chunk));
    }
}

#[tokio::test]
async fn a_scan_stops_at_the_limit() {
    let fixture = Fixture::new(&[(tree(1), 3000), (tree(2), 3000)], 1 << 20).await;
    let expected = fixture.states(|row| row.category == "step-3");

    let states = fixture.scan_up_to(filter("step-3"), 5).await;

    assert_eq!(states, expected[..5]);
    let payload = fixture.payload_column();
    let last_page = pages(&fixture.metadata())
        .into_iter()
        .rfind(|page| page.column == payload)
        .unwrap();
    assert!(
        !fetched(&fixture.store, &last_page.bytes),
        "no payload after the limit"
    );
}

#[tokio::test]
async fn only_the_payload_pages_of_the_passing_states_are_fetched() {
    let fixture = Fixture::new(&[(tree(1), 3000), (tree(2), 3000)], 1 << 20).await;
    let payload = fixture.payload_column();
    // Row 17 of each tree, the only rows with this label.
    let passing: Vec<i64> = (0..fixture.rows.len() as i64)
        .filter(|&row| fixture.rows[row as usize].time == UnixMillis::from(FIRST_MS + 17))
        .collect();
    let pages: Vec<Page> = pages(&fixture.metadata())
        .into_iter()
        .filter(|page| page.column == payload)
        .collect();
    assert!(pages.len() > 2, "{} payload pages", pages.len());
    let only_17 = StatesFilter {
        meta: labels(&[("n", "17")]),
        ..filter("step-3")
    };

    let states = fixture.scan(only_17).await;

    assert_eq!(states.len(), passing.len());
    for page in pages {
        let holds_a_state = passing.iter().any(|row| page.rows.contains(row));
        assert_eq!(
            fetched(&fixture.store, &page.bytes),
            holds_a_state,
            "payload page of rows {:?}",
            page.rows
        );
    }
}

#[tokio::test]
async fn rejects_a_file_without_a_column_whatever_rows_pass() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = written
        .rewritten(layout(Some(SCHEMA_VERSION)).build(), |mut batch| {
            batch.remove_column(batch.schema().index_of(StatesColumn::Meta.name()).unwrap());
            batch
        })
        .await;
    let filters = [
        ("some rows pass", filter("step-3")),
        ("no row passes", filter("step-9")),
        (
            "the column is in the filter",
            StatesFilter {
                meta: labels(&[("n", "3")]),
                ..filter("step-3")
            },
        ),
    ];

    for (case, filter) in filters {
        let error = fixture
            .scan_with(FileReadConfig::default(), filter)
            .await
            .unwrap_err();

        assert_eq!(error.file(), &fixture.name);
        assert!(
            matches!(
                error.problem(),
                StatesFileProblem::File(FileProblem::Column { column: "meta" })
            ),
            "{case}: {error:?}"
        );
    }
}

#[tokio::test]
async fn reports_a_bad_payload_of_a_passing_state_and_says_which_row() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = with_values(&written, StatesColumn::PayloadType, |rows| {
        Arc::new(StringArray::from(vec!["yaml"; rows]))
    })
    .await;

    let error = fixture
        .scan_with(FileReadConfig::default(), filter("step-3"))
        .await
        .unwrap_err();

    assert!(matches!(
        error.problem(),
        StatesFileProblem::Row {
            problem: InvalidRow::UnknownPayloadType(_),
            ..
        }
    ));
}

#[tokio::test]
async fn rejects_a_tree_column_of_another_width() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = with_values(&written, StatesColumn::Tree, |rows| {
        let wide = (0..rows).map(|_| [2; 16]);
        Arc::new(FixedSizeBinaryArray::try_from_iter(wide).unwrap())
    })
    .await;

    let error = fixture
        .scan_with(FileReadConfig::default(), filter("step-3"))
        .await
        .unwrap_err();

    assert!(matches!(
        error.problem(),
        StatesFileProblem::File(FileProblem::Column { column: "tree" })
    ));
}

#[tokio::test]
async fn rejects_a_column_of_another_type() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = with_values(&written, StatesColumn::Ts, |rows| {
        Arc::new(StringArray::from(vec!["noon"; rows]))
    })
    .await;

    let error = fixture
        .scan_with(FileReadConfig::default(), filter("step-3"))
        .await
        .unwrap_err();

    assert!(matches!(
        error.problem(),
        StatesFileProblem::File(FileProblem::Column { column: "ts" })
    ));
}

#[tokio::test]
async fn rejects_an_unknown_kind_of_a_row_that_passes() {
    let written = Fixture::new(&[(tree(2), 10)], 1 << 20).await;
    let fixture = with_values(&written, StatesColumn::Kind, |rows| {
        Arc::new(StringArray::from(vec!["stat"; rows]))
    })
    .await;

    let error = fixture
        .scan_with(FileReadConfig::default(), filter("step-3"))
        .await
        .unwrap_err();

    assert!(
        matches!(
            error.problem(),
            StatesFileProblem::Row {
                problem: InvalidRow::UnknownKind(kind),
                ..
            } if kind == "stat"
        ),
        "{error:?}"
    );
}
