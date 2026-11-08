//! Why an expression is not a filter.

use std::fmt;

use super::expression::Scalar;

/// Why an expression is not a filter, and where in it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind} at column {column}")]
pub struct FilterError {
    /// What is wrong.
    pub kind: FilterErrorKind,
    /// Where it is, thus a client can point to it. It counts Unicode
    /// characters (scalar values) from 1. A client that counts UTF-16 units,
    /// or cells of a screen, must convert it for text with other than ASCII
    /// characters.
    pub column: usize,
}

/// What is wrong with an expression.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FilterErrorKind {
    /// The expression does not start with a path.
    #[error("expected a path: a key, a subscript or `.`")]
    ExpectedPath,
    /// `.` is the root only when it is the full path.
    #[error("`.` is the root and cannot have more steps; start the path with a key or a subscript")]
    StepAfterRoot,
    /// A `.` in a path without a key after it.
    #[error("expected a key after `.`")]
    ExpectedKey,
    /// A `[` without a valid subscript after it.
    #[error("expected `[]`, `[n]`, `[-n]` or `[\"key\"]`")]
    ExpectedSubscript,
    /// An index that no array can have.
    #[error("the index is too large")]
    IndexTooLarge,
    /// `[-0]`, which names no element.
    #[error("`[-0]` is not an index; `[-1]` is the last element")]
    NegativeZero,
    /// The path is not followed by an operator.
    #[error("expected `=`, `!=`, `<`, `<=`, `>`, `>=` or `in`")]
    ExpectedOperator,
    /// `in` without a space after it.
    #[error("expected a space after `in`")]
    ExpectedSpace,
    /// No value where one is necessary.
    #[error("expected a string, a number, `true`, `false` or `null`")]
    ExpectedScalar,
    /// A number that is not a JSON number.
    #[error("invalid number: {0}")]
    InvalidNumber(String),
    /// A string that is not a JSON string.
    #[error("invalid string: {0}")]
    InvalidString(String),
    /// A string without its closing quote.
    #[error("the string has no closing quote")]
    UnclosedString,
    /// The first bound of a range is not followed by `..` or `..=`.
    #[error("expected `..` or `..=`")]
    ExpectedRange,
    /// The two bounds of a range are of different types.
    #[error("the bounds of a range must have the same type, not {start} and {end}")]
    MixedRange {
        /// The type of the start.
        start: ScalarType,
        /// The type of the end.
        end: ScalarType,
    },
    /// The bounds of a range are of a type without order.
    #[error("a range of {0} values has no order; use numbers or strings")]
    UnorderedRange(ScalarType),
    /// No value can be in the range. Refused, because the filter would give
    /// an empty answer, which a caller could take for "no state matches".
    #[error("the range holds no value, since its start is not before its end")]
    EmptyRange,
    /// The expression goes on after a full filter.
    #[error("unexpected text after the filter")]
    TrailingText,
}

/// The type of a JSON scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    /// A string.
    String,
    /// A number.
    Number,
    /// `true` or `false`.
    Bool,
    /// `null`.
    Null,
}

impl From<&Scalar> for ScalarType {
    fn from(scalar: &Scalar) -> Self {
        match scalar {
            Scalar::String(_) => ScalarType::String,
            Scalar::Number(_) => ScalarType::Number,
            Scalar::Bool(_) => ScalarType::Bool,
            Scalar::Null => ScalarType::Null,
        }
    }
}

impl fmt::Display for ScalarType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ScalarType::String => "string",
            ScalarType::Number => "number",
            ScalarType::Bool => "boolean",
            ScalarType::Null => "null",
        })
    }
}
