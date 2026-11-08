//! A parsed filter expression.

use std::cmp::Ordering;
use std::num::NonZeroUsize;

use serde_json::Number;

use super::number::compare_numbers;

/// A test on one value of the payload: `path cmp scalar`, or
/// `path in scalar..scalar`.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    /// Where the values are in the payload.
    pub path: Path,
    /// What a value must be.
    pub test: Test,
}

/// The steps from the root of the payload to the values that are tested.
/// No step is the root itself.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Path(pub Vec<Step>);

/// One step of a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The value of a key of an object.
    Key(String),
    /// One element of an array.
    Index(Index),
    /// Each element of an array, `[]`. A filter matches when one of the
    /// elements matches.
    Any,
}

/// The position of an element in an array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {
    /// `[n]`, counted from zero.
    FromStart(usize),
    /// `[-n]`, where `[-1]` is the last element. There is no `[-0]`.
    FromEnd(NonZeroUsize),
}

/// What a value must be.
#[derive(Debug, Clone, PartialEq)]
pub enum Test {
    /// A comparison with a JSON scalar.
    Compare(Comparison, Scalar),
    /// A position between two bounds of the same orderable type.
    Range {
        /// The two bounds.
        bounds: RangeBounds,
        /// Whether the end is in the range.
        end: RangeEnd,
    },
}

/// A comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

/// A JSON scalar of an expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    /// A string.
    String(String),
    /// A number. An integer that fits 64 bits stays exact, thus it compares
    /// exactly with an integer of the payload. A larger one is a float;
    /// such ids are sent as strings. JSON reads `-0` as a float, which is
    /// equal to `0`.
    Number(Number),
    /// `true` or `false`.
    Bool(bool),
    /// `null`.
    Null,
}

/// The bounds of a range. Only numbers and strings have an order, and both
/// bounds have the same type.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeBounds {
    /// Two numbers.
    Numbers(Number, Number),
    /// Two strings, in the order of their UTF-8 bytes.
    Strings(String, String),
}

impl RangeBounds {
    /// Whether no value can be in the range: its start is after its end, or
    /// at its end when the end is not in the range.
    pub(super) fn is_empty(&self, kind: RangeEnd) -> bool {
        let order = match self {
            RangeBounds::Numbers(start, end) => compare_numbers(start, end),
            RangeBounds::Strings(start, end) => start.as_bytes().cmp(end.as_bytes()),
        };
        match kind {
            RangeEnd::Exclusive => order != Ordering::Less,
            RangeEnd::Inclusive => order == Ordering::Greater,
        }
    }
}

/// Whether the end of a range is in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeEnd {
    /// `..`: the end is not in the range.
    Exclusive,
    /// `..=`: the end is in the range.
    Inclusive,
}
