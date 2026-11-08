//! The order of JSON numbers.

use std::cmp::Ordering;

use serde_json::Number;

/// The order of `a` and `b` by value. Two integers compare exactly, since
/// a float cannot hold each integer of 64 bits. Other numbers compare as
/// floats, thus `1` and `1.0` are equal, and `-0` and `0` are too.
pub(crate) fn compare_numbers(a: &Number, b: &Number) -> Ordering {
    match (integer(a), integer(b)) {
        (Some(a), Some(b)) => a.cmp(&b),
        // A JSON number is never NaN, thus floats always have an order.
        _ => float(a).partial_cmp(&float(b)).unwrap_or(Ordering::Equal),
    }
}

fn integer(number: &Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

/// Each number has a float value, unless serde_json keeps numbers as text,
/// which this crate does not ask it to.
fn float(number: &Number) -> f64 {
    number.as_f64().unwrap_or(f64::NAN)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn number(text: &str) -> Number {
        text.parse().unwrap()
    }

    #[test]
    fn integers_compare_exactly() {
        assert_eq!(
            compare_numbers(&number("9007199254740993"), &number("9007199254740992")),
            Ordering::Greater
        );
        assert_eq!(
            compare_numbers(&number("-1"), &number("18446744073709551615")),
            Ordering::Less
        );
    }

    #[test]
    fn an_integer_and_a_float_compare_by_value() {
        assert_eq!(
            compare_numbers(&number("1"), &number("1.0")),
            Ordering::Equal
        );
        assert_eq!(
            compare_numbers(&number("-0"), &number("0")),
            Ordering::Equal
        );
        assert_eq!(
            compare_numbers(&number("2"), &number("1.5")),
            Ordering::Greater
        );
    }
}
