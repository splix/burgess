//! The number of each decoded row in its file.
//!
//! A cursor names the last row given by its file and its number. The number
//! must be the same on each page, thus it is the row number that the
//! decoder gives, not a count of the rows that passed: the filters of two
//! pages can be different.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef};
use parquet::arrow::RowNumber;

use super::problem::FileProblem;

/// The name of the column that the decoder adds. Not a column of the
/// writer, thus it cannot hide one.
const ROW_NUMBER: &str = "_burgess_row_number";

/// A row and its number in the file, from 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Numbered<T> {
    /// The number of the row in the file.
    pub number: u64,
    /// The row.
    pub row: T,
}

/// The column that the decoder adds to each batch.
pub(super) fn row_number_field() -> FieldRef {
    Arc::new(Field::new(ROW_NUMBER, DataType::Int64, false).with_extension_type(RowNumber))
}

/// The row numbers of `batch`.
///
/// A cursor that holds a wrong number repeats or skips rows on the next
/// page, thus a number that cannot be a row number is a problem of the
/// file, not a row 0.
pub(crate) fn row_numbers(batch: &RecordBatch) -> Result<RowNumbers<'_>, FileProblem> {
    let numbers = batch
        .column_by_name(ROW_NUMBER)
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or(FileProblem::Column { column: ROW_NUMBER })?;
    if numbers.null_count() > 0 {
        return Err(FileProblem::RowNumber { found: None });
    }
    if let Some(&negative) = numbers.values().iter().find(|number| **number < 0) {
        return Err(FileProblem::RowNumber {
            found: Some(negative),
        });
    }
    Ok(RowNumbers(numbers))
}

/// The row numbers of a batch, none of them below 0.
#[derive(Clone, Copy)]
pub(crate) struct RowNumbers<'a>(&'a Int64Array);

impl RowNumbers<'_> {
    /// The number of `row` of the batch in its file.
    pub(crate) fn of(&self, row: usize) -> u64 {
        // No number is below 0, thus this is the number.
        self.0.value(row).unsigned_abs()
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;

    use super::*;

    fn batch(numbers: Vec<Option<i64>>) -> RecordBatch {
        let field = Field::new(ROW_NUMBER, DataType::Int64, true);
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(Int64Array::from(numbers))],
        )
        .unwrap()
    }

    #[test]
    fn the_numbers_of_the_decoder_are_the_row_numbers() {
        let batch = batch(vec![Some(0), Some(7), Some(i64::MAX)]);
        let numbers = row_numbers(&batch).unwrap();
        let read: Vec<u64> = (0..3).map(|row| numbers.of(row)).collect();
        assert_eq!(read, [0, 7, i64::MAX as u64]);
    }

    /// A cursor after such a row would repeat or skip rows.
    #[test]
    fn a_number_that_no_row_can_have_is_a_problem_of_the_file() {
        for (numbers, found) in [
            (vec![Some(0), Some(-1)], Some(-1)),
            (vec![Some(0), None], None),
        ] {
            let batch = batch(numbers);
            assert!(
                matches!(row_numbers(&batch), Err(FileProblem::RowNumber { found: f }) if f == found),
                "{found:?}"
            );
        }
    }
}
