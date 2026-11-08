//! The order of the values that a filter can order: numbers and strings.

use std::cmp::Ordering;

use serde_json::{Number, Value};

/// A number or a string, of a filter or of a payload.
#[derive(Debug, Clone, Copy)]
pub(super) enum Orderable<'a> {
    Number(&'a Number),
    String(&'a str),
}

impl<'a> Orderable<'a> {
    /// `value` if it is a number or a string.
    pub(super) fn of_value(value: &'a Value) -> Option<Self> {
        match value {
            Value::Number(number) => Some(Orderable::Number(number)),
            Value::String(string) => Some(Orderable::String(string)),
            _ => None,
        }
    }

    /// The order of `self` and `other`, or `None` when they are of different
    /// types, which never match.
    ///
    /// Two integers compare exactly, since a float cannot hold each integer
    /// of 64 bits. Other numbers compare as floats, thus `1` and `1.0` are
    /// equal, and `-0` and `0` are too. Strings compare by their UTF-8
    /// bytes.
    pub(super) fn order(self, other: Orderable<'_>) -> Option<Ordering> {
        match (self, other) {
            (Orderable::Number(a), Orderable::Number(b)) => match (integer(a), integer(b)) {
                (Some(a), Some(b)) => Some(a.cmp(&b)),
                // A number without a float value has no order, thus it
                // matches nothing, and not everything.
                _ => a.as_f64()?.partial_cmp(&b.as_f64()?),
            },
            (Orderable::String(a), Orderable::String(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
            _ => None,
        }
    }
}

fn integer(number: &Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn number(text: &str) -> Number {
        text.parse().unwrap()
    }

    fn order(a: &str, b: &str) -> Option<Ordering> {
        Orderable::Number(&number(a)).order(Orderable::Number(&number(b)))
    }

    #[test]
    fn integers_compare_exactly() {
        assert_eq!(
            order("9007199254740993", "9007199254740992"),
            Some(Ordering::Greater)
        );
        assert_eq!(order("-1", "18446744073709551615"), Some(Ordering::Less));
    }

    #[test]
    fn an_integer_and_a_float_compare_by_value() {
        assert_eq!(order("1", "1.0"), Some(Ordering::Equal));
        assert_eq!(order("-0", "0"), Some(Ordering::Equal));
        assert_eq!(order("2", "1.5"), Some(Ordering::Greater));
    }

    #[test]
    fn strings_compare_by_bytes() {
        let order = |a, b| Orderable::String(a).order(Orderable::String(b));
        assert_eq!(order("B", "a"), Some(Ordering::Less));
        assert_eq!(order("é", "z"), Some(Ordering::Greater));
    }

    #[test]
    fn a_number_and_a_string_have_no_order() {
        let five = number("5");
        assert_eq!(Orderable::Number(&five).order(Orderable::String("5")), None);
    }
}
