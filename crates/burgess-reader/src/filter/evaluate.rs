//! Whether a payload passes a filter.

use std::cmp::Ordering;

use serde_json::Value;

use super::expression::{Comparison, Filter, Index, RangeBounds, RangeEnd, Scalar, Step, Test};
use super::order::Orderable;

impl Filter {
    /// Whether one of the values at the path of `payload` passes the test.
    ///
    /// Each `[]` of the path gives each element of its array, and one value
    /// that passes is enough, also for `!=`. A path that is not in the
    /// payload gives no value, thus no match.
    ///
    /// The values are visited one at a time, from a list of the values to
    /// visit and not by recursion, thus a long path cannot use up the stack.
    /// The visit stops at the first value that passes.
    pub fn matches(&self, payload: &Value) -> bool {
        let steps = &self.path.0;
        let mut to_visit = vec![(payload, 0)];
        while let Some((value, at)) = to_visit.pop() {
            let Some(step) = steps.get(at) else {
                if self.test.accepts(value) {
                    return true;
                }
                continue;
            };
            let first = to_visit.len();
            step.follow(value, |next| to_visit.push((next, at + 1)));
            // The list is a stack, thus the first element is visited first.
            to_visit[first..].reverse();
        }
        false
    }
}

impl Step {
    /// Gives `found` each value that the step gives from `value`. A key of
    /// an object that is not there, an index out of the array, or a value
    /// of another type gives none.
    fn follow<'a>(&self, value: &'a Value, found: impl FnMut(&'a Value)) {
        match (self, value) {
            (Step::Key(key), Value::Object(object)) => object.get(key).into_iter().for_each(found),
            (Step::Index(index), Value::Array(array)) => index
                .of(array.len())
                .map(|at| &array[at])
                .into_iter()
                .for_each(found),
            (Step::Any, Value::Array(array)) => array.iter().for_each(found),
            _ => {}
        }
    }
}

impl Index {
    /// The position in an array of `len` elements, if it has one.
    fn of(self, len: usize) -> Option<usize> {
        match self {
            Index::FromStart(at) => (at < len).then_some(at),
            Index::FromEnd(back) => len.checked_sub(back.get()),
        }
    }
}

impl Test {
    /// Whether `value` passes. A value of another type than the scalars of
    /// the test never passes, for each operator: there is no coercion, and
    /// `"5" != 5` is not a match.
    fn accepts(&self, value: &Value) -> bool {
        match self {
            Test::Compare(comparison, scalar) => {
                order(scalar, value).is_some_and(|order| comparison.passes(order))
            }
            Test::Range { bounds, end } => bounds.contains(value, *end),
        }
    }
}

/// How `value` compares with `scalar`, when they have the same type.
fn order(scalar: &Scalar, value: &Value) -> Option<Order> {
    let scalar = match (scalar, value) {
        (Scalar::Number(number), _) => Orderable::Number(number),
        (Scalar::String(string), _) => Orderable::String(string),
        (Scalar::Bool(scalar), Value::Bool(value)) => {
            return Some(Order::Unordered {
                equal: scalar == value,
            });
        }
        (Scalar::Null, Value::Null) => return Some(Order::Unordered { equal: true }),
        _ => return None,
    };
    Orderable::of_value(value)?
        .order(scalar)
        .map(Order::Ordered)
}

/// How a value compares with a scalar of the same type.
enum Order {
    /// Numbers and strings.
    Ordered(Ordering),
    /// Bool and null, which have no order.
    Unordered { equal: bool },
}

impl Comparison {
    /// Whether a value in `order` with the scalar passes.
    fn passes(self, order: Order) -> bool {
        match (self, order) {
            (Comparison::Eq, Order::Ordered(order)) => order == Ordering::Equal,
            (Comparison::Ne, Order::Ordered(order)) => order != Ordering::Equal,
            (Comparison::Eq, Order::Unordered { equal }) => equal,
            (Comparison::Ne, Order::Unordered { equal }) => !equal,
            (Comparison::Lt, Order::Ordered(order)) => order == Ordering::Less,
            (Comparison::Le, Order::Ordered(order)) => order != Ordering::Greater,
            (Comparison::Gt, Order::Ordered(order)) => order == Ordering::Greater,
            (Comparison::Ge, Order::Ordered(order)) => order != Ordering::Less,
            (
                Comparison::Lt | Comparison::Le | Comparison::Gt | Comparison::Ge,
                Order::Unordered { .. },
            ) => false,
        }
    }
}

impl RangeBounds {
    /// Whether `value` is in the range. Both bounds are tested on the same
    /// value, thus on the same element of each `[]`.
    fn contains(&self, value: &Value, kind: RangeEnd) -> bool {
        let Some(value) = Orderable::of_value(value) else {
            return false;
        };
        let (start, end) = self.orderable();
        let (Some(from_start), Some(to_end)) = (value.order(start), value.order(end)) else {
            return false;
        };
        from_start != Ordering::Less
            && match kind {
                RangeEnd::Exclusive => to_end == Ordering::Less,
                RangeEnd::Inclusive => to_end != Ordering::Greater,
            }
    }
}
