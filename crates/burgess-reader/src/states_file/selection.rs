//! Which rows of a file can hold a tree, by the statistics on `tree`.

use burgess_core::flow::TreeId;
use parquet::arrow::arrow_reader::{RowGroupSelection, RowSelection, RowSelector};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;

/// The rows to fetch for one tree: whole pages of `tree` whose bounds
/// include it, in the row groups whose bounds include it.
///
/// Pages are the unit the store is read in, so a selected page may also
/// hold rows of neighbouring trees. They are dropped after decoding.
#[derive(Debug)]
pub(super) struct TreeSelection {
    /// Row groups to read, in file order, each with the rows to read in it.
    /// Rows are counted within each row group, so a damaged page index in
    /// one cannot misplace the rows read from another.
    pub(super) row_groups: Vec<RowGroupSelection>,
}

impl TreeSelection {
    /// The selection for `tree`, or `None` when the statistics rule out
    /// every row. `tree_column` is the leaf index of the `tree` column.
    ///
    /// Bounds that are missing or inconsistent cannot rule anything out, so
    /// such a row group is read whole rather than wrongly found empty.
    pub(super) fn find(
        metadata: &ParquetMetaData,
        tree_column: usize,
        tree: TreeId,
    ) -> Option<Self> {
        let row_groups: Vec<RowGroupSelection> = metadata
            .row_groups()
            .iter()
            .enumerate()
            .filter_map(|(index, row_group)| {
                let statistics = row_group.column(tree_column).statistics();
                let bounds = Bounds {
                    min: statistics.and_then(|stats| stats.min_bytes_opt()),
                    max: statistics.and_then(|stats| stats.max_bytes_opt()),
                };
                if !bounds.may_hold(tree) {
                    return None;
                }
                match page_selectors(metadata, index, row_group, tree_column, tree) {
                    None => Some(RowGroupSelection::new(index, None)),
                    Some(pages) if pages.iter().any(|page| !page.skip) => Some(
                        RowGroupSelection::new(index, Some(RowSelection::from(pages))),
                    ),
                    Some(_) => None,
                }
            })
            .collect();
        (!row_groups.is_empty()).then_some(TreeSelection { row_groups })
    }
}

/// The smallest and largest tree in some rows, as raw id bytes.
struct Bounds<'a> {
    min: Option<&'a [u8]>,
    max: Option<&'a [u8]>,
}

impl Bounds<'_> {
    /// Whether the rows may hold `tree`. The file stores ids as their raw
    /// bytes, which compare the same way the ids sort.
    fn may_hold(&self, tree: TreeId) -> bool {
        let tree = tree.as_bytes().as_slice();
        self.min.is_none_or(|min| min <= tree) && self.max.is_none_or(|max| tree <= max)
    }
}

/// One selector per page of `tree` in a row group, or `None` when the row
/// group has no usable page index.
fn page_selectors(
    metadata: &ParquetMetaData,
    index: usize,
    row_group: &RowGroupMetaData,
    tree_column: usize,
    tree: TreeId,
) -> Option<Vec<RowSelector>> {
    let page_index = metadata.page_index_for_row_group(index);
    let Some(ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(bounds)) =
        page_index.column_index(tree_column)
    else {
        return None;
    };
    let first_rows: Vec<i64> = page_index
        .page_locations(tree_column)?
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
        .may_hold(tree)
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
