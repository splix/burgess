//! Which rows of a file can hold a key, by the statistics on its columns.

use parquet::arrow::arrow_reader::{RowGroupSelection, RowSelection, RowSelector};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;

/// The value that one key column must have, as the bytes it is stored as.
///
/// A file is sorted by its key columns, thus their bounds are narrow. String
/// bounds can be truncated by the writer. A truncated minimum is a prefix and
/// a truncated maximum is made larger, thus they stay correct bounds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ColumnKey<'a> {
    /// The leaf index of the column.
    pub(crate) column: usize,
    /// The value that a row must have.
    pub(crate) value: &'a [u8],
}

/// The rows to fetch for a key: the pages whose bounds include the key, in
/// the row groups whose bounds include it.
///
/// Pages are the unit the store is read in, so a selected page may also
/// hold rows of other keys. They are dropped after decoding.
#[derive(Debug)]
pub(crate) struct KeySelection {
    /// Row groups to read, in file order, each with the rows to read in it.
    /// Rows are counted within each row group, so a damaged page index in
    /// one cannot misplace the rows read from another.
    pub(crate) row_groups: Vec<RowGroupSelection>,
}

impl KeySelection {
    /// The selection for a row that has every value of `keys`, or `None`
    /// when the statistics rule out every row.
    ///
    /// A row can hold the key only when the bounds of each key column
    /// include its value. The key columns have different page boundaries,
    /// thus the rows of each column are selected and the selections are
    /// intersected. Bounds that are missing or inconsistent cannot rule
    /// anything out, so they select all rows rather than wrongly none.
    pub(crate) fn find(metadata: &ParquetMetaData, keys: &[ColumnKey<'_>]) -> Option<Self> {
        let row_groups: Vec<RowGroupSelection> = metadata
            .row_groups()
            .iter()
            .enumerate()
            .filter(|(_, row_group)| keys.iter().all(|key| row_group_may_hold(row_group, *key)))
            .filter_map(|(index, row_group)| {
                let pages = keys
                    .iter()
                    .filter_map(|key| page_selectors(metadata, index, row_group, *key))
                    .map(RowSelection::from)
                    .reduce(|selected, other| selected.intersection(&other));
                match pages {
                    None => Some(RowGroupSelection::new(index, None)),
                    Some(pages) if pages.selects_any() => {
                        Some(RowGroupSelection::new(index, Some(pages)))
                    }
                    Some(_) => None,
                }
            })
            .collect();
        (!row_groups.is_empty()).then_some(KeySelection { row_groups })
    }
}

fn row_group_may_hold(row_group: &RowGroupMetaData, key: ColumnKey<'_>) -> bool {
    let statistics = row_group.column(key.column).statistics();
    Bounds {
        min: statistics.and_then(|stats| stats.min_bytes_opt()),
        max: statistics.and_then(|stats| stats.max_bytes_opt()),
    }
    .may_hold(key.value)
}

/// The smallest and largest value in some rows, as stored bytes.
struct Bounds<'a> {
    min: Option<&'a [u8]>,
    max: Option<&'a [u8]>,
}

impl Bounds<'_> {
    /// Whether the rows may hold `value`. Ids and strings are stored so that
    /// their bytes compare the same way that the values sort.
    fn may_hold(&self, value: &[u8]) -> bool {
        self.min.is_none_or(|min| min <= value) && self.max.is_none_or(|max| value <= max)
    }
}

/// One selector per page of the key column in a row group, or `None` when
/// the row group has no usable page index for it.
fn page_selectors(
    metadata: &ParquetMetaData,
    index: usize,
    row_group: &RowGroupMetaData,
    key: ColumnKey<'_>,
) -> Option<Vec<RowSelector>> {
    let page_index = metadata.page_index_for_row_group(index);
    let bounds = match page_index.column_index(key.column)? {
        ColumnIndexMetaData::BYTE_ARRAY(bounds)
        | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(bounds) => bounds,
        _ => return None,
    };
    let first_rows: Vec<i64> = page_index
        .page_locations(key.column)?
        .iter()
        .map(|location| location.first_row_index)
        .collect();
    if bounds.num_pages() != first_rows.len() as u64 {
        return None;
    }
    pages_to_rows(&first_rows, row_group.num_rows(), |page| {
        Bounds {
            min: bounds.min_value(page),
            max: bounds.max_value(page),
        }
        .may_hold(key.value)
    })
}

/// Selects the rows of the pages starting at `first_rows` that `wanted`
/// picks, and skips the rest.
///
/// `None` when the pages do not split the row group's rows into ascending,
/// non-empty runs from zero, which only a damaged offset index gives.
fn pages_to_rows(
    first_rows: &[i64],
    num_rows: i64,
    wanted: impl Fn(usize) -> bool,
) -> Option<Vec<RowSelector>> {
    if first_rows.first() != Some(&0) {
        return None;
    }
    let ends = first_rows.iter().skip(1).copied().chain([num_rows]);
    first_rows
        .iter()
        .zip(ends)
        .enumerate()
        .map(|(page, (&start, end))| {
            let rows = usize::try_from(end - start).ok().filter(|&rows| rows > 0)?;
            Some(if wanted(page) {
                RowSelector::select(rows)
            } else {
                RowSelector::skip(rows)
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_the_wanted_pages() {
        let selectors = pages_to_rows(&[0, 10, 25], 30, |page| page == 1);
        assert_eq!(
            selectors,
            Some(vec![
                RowSelector::skip(10),
                RowSelector::select(15),
                RowSelector::skip(5),
            ])
        );
    }

    #[test]
    fn pages_not_starting_at_zero_are_unusable() {
        assert_eq!(pages_to_rows(&[5, 10], 30, |_| true), None);
    }

    #[test]
    fn descending_page_starts_are_unusable() {
        assert_eq!(pages_to_rows(&[0, 20, 10], 30, |_| true), None);
    }

    #[test]
    fn pages_past_the_row_group_end_are_unusable() {
        assert_eq!(pages_to_rows(&[0, 10, 40], 30, |_| true), None);
    }

    #[test]
    fn a_row_group_without_pages_is_unusable() {
        assert_eq!(pages_to_rows(&[], 30, |_| true), None);
    }
}
