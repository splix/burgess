//! The filter expression of a ListStates request, on the payload of a
//! state.
//!
//! The service parses the expression, thus each client gets the same rules
//! and the same errors, and a client only shows the error. An expression
//! that does not follow the grammar is refused with its error. It never
//! gives an empty answer, which a caller could take for "no state matches".

mod error;
mod expression;
mod number;
mod parser;

pub use error::{FilterError, FilterErrorKind, ScalarType};
pub use expression::{Comparison, Filter, Index, Path, RangeBounds, RangeEnd, Scalar, Step, Test};

#[cfg(test)]
mod tests;
