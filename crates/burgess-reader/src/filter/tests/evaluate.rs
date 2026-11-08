//! Which payloads a filter matches.

use serde_json::{Value, json};

use super::*;

/// Whether `expression` matches `payload`.
fn matches(expression: &str, payload: &Value) -> bool {
    parse(expression).matches(payload)
}

/// The expressions of `cases` that match `payload`.
fn matching<'a>(payload: &Value, cases: &[&'a str]) -> Vec<&'a str> {
    cases
        .iter()
        .copied()
        .filter(|expression| matches(expression, payload))
        .collect()
}

#[test]
fn a_path_that_is_not_in_the_payload_does_not_match() {
    let payload = json!({
        "tx": {"outputs": [{"address": "0xabc"}, {"address": "0xdef"}]},
        "headers": {"content-type": "text/plain"},
        "status": "ok",
    });

    let cases = [
        r#"tx.outputs[-1].address = "0xdef""#,
        r#"tx.outputs[-2].address = "0xabc""#,
        r#"tx.outputs[0].address = "0xabc""#,
        r#"headers["content-type"] = "text/plain""#,
        // The rest is not in the payload.
        r#"tx.outputs[2].address = "0xabc""#,
        r#"tx.outputs[-3].address = "0xabc""#,
        r#"tx.inputs[].address = "0xabc""#,
        r#"status.code = "ok""#,
        r#"status[0] = "ok""#,
        r#"tx[0] != "x""#,
        r#"tx.outputs.address != "x""#,
    ];

    assert_eq!(
        matching(&payload, &cases),
        cases[..4],
        "a missing key, an index out of the array, or the wrong type is no match"
    );
}

#[test]
fn a_path_can_be_the_root_or_start_with_a_subscript() {
    assert!(matches(r#". = "timeout""#, &json!("timeout")));
    assert!(!matches(r#". = "timeout""#, &json!({"error": "timeout"})));
    assert!(matches("[0].code = 500", &json!([{"code": 500}])));
    assert!(matches("[-1] = 3", &json!([1, 2, 3])));
}

#[test]
fn one_element_of_each_any_is_enough_also_for_not_equal() {
    let payload = json!({"inputs": [{"value": 5}, {"value": 2000}]});

    assert!(matches("inputs[].value >= 1000", &payload));
    assert!(matches("inputs[].value < 10", &payload));
    assert!(
        matches("inputs[].value != 5", &payload),
        "the element 2000 is not 5"
    );
    assert!(!matches(
        "inputs[].value != 5",
        &json!({"inputs": [{"value": 5}]})
    ));
    assert!(!matches("inputs[].value = 1", &json!({"inputs": []})));
}

#[test]
fn each_combination_of_two_anys_is_tried() {
    let payload = json!({"blocks": [{"tx": [1, 2]}, {"tx": [3, 40]}]});

    assert!(matches("blocks[].tx[] = 40", &payload));
    assert!(matches("blocks[].tx[-1] in 30..50", &payload));
    assert!(!matches("blocks[].tx[0] > 3", &payload));
}

#[test]
fn a_value_of_another_type_never_matches() {
    let payload = json!({"count": 5, "code": "5", "ok": true, "error": null});

    let cases = [
        "count = 5",
        "count = 5.0",
        r#"code = "5""#,
        "ok = true",
        "error = null",
        // The rest compare values of another type.
        r#"count = "5""#,
        r#"count != "5""#,
        "code = 5",
        "code != 5",
        "ok = 1",
        "ok != 1",
        "count != null",
        "error != 5",
        "error != false",
        "count != true",
    ];

    assert_eq!(matching(&payload, &cases), cases[..5]);
}

#[test]
fn numbers_are_one_type() {
    assert!(matches("a = 1.0", &json!({"a": 1})));
    assert!(matches("a = 1", &json!({"a": 1.0})));
    assert!(matches("a = 0", &json!({"a": -0.0})));
    assert!(!matches("a != 1", &json!({"a": 1.0})));
    assert!(matches("a = -3", &json!({"a": -3})));
}

#[test]
fn not_equal_matches_a_value_of_the_same_type() {
    let payload = json!({"s": "a", "n": 1, "b": true, "z": null});

    assert!(matches(r#"s != "b""#, &payload));
    assert!(matches("n != 2", &payload));
    assert!(matches("b != false", &payload));
    assert!(!matches("z != null", &payload), "`!= null` never matches");
}

#[test]
fn numbers_order_by_value_and_strings_by_bytes() {
    let payload = json!({"n": 9007199254740993_u64, "f": 2.5, "s": "b", "e": "é"});

    let cases = [
        "n > 9007199254740992",
        "n <= 9007199254740993",
        "f > 2",
        "f < 3",
        "f >= 2.5",
        r#"s > "a""#,
        r#"s < "c""#,
        r#"s >= "b""#,
        r#"e > "z""#,
        // The rest are false.
        "n < 9007199254740993",
        "f > 2.5",
        r#"s < "B""#,
        r#"s > "ba""#,
    ];

    assert_eq!(matching(&payload, &cases), cases[..9]);
}

#[test]
fn bool_and_null_never_match_an_ordering_operator() {
    let payload = json!({"b": true, "z": null});

    for expression in [
        "b > false",
        "b >= true",
        "b <= true",
        "b < true",
        "z <= null",
        "z >= null",
        "z < null",
    ] {
        assert!(!matches(expression, &payload), "{expression}");
    }
}

#[test]
fn a_range_without_its_end_excludes_it() {
    let at = |fee: f64| json!({"fee": fee});

    assert!(matches("fee in 10..20", &at(10.0)));
    assert!(matches("fee in 10..20", &at(19.5)));
    assert!(!matches("fee in 10..20", &at(20.0)));
    assert!(!matches("fee in 10..20", &at(9.9)));
    assert!(matches("fee in 10..=20", &at(20.0)));
    assert!(!matches("fee in 10..=20", &at(20.1)));
    assert!(matches(r#"name in "a".."b""#, &json!({"name": "az"})));
    assert!(!matches(r#"name in "a".."b""#, &json!({"name": "b"})));
    assert!(!matches("fee in 10..20", &json!({"fee": "15"})));
}

#[test]
fn both_bounds_are_on_the_same_element() {
    // No element is in the range, though one is above 10 and one below 20.
    let payload = json!({"fees": [5, 25]});

    assert!(!matches("fees[] in 10..20", &payload));
    assert!(matches("fees[] in 10..30", &payload));
}

#[test]
fn big_integers_sent_as_strings_compare_as_strings() {
    let payload = json!({"id": "100000000000000000000"});

    assert!(matches(r#"id = "100000000000000000000""#, &payload));
    assert!(matches(r#"id < "9""#, &payload), "by bytes, not by value");
    assert!(!matches("id > 9", &payload), "a number is another type");
}

#[test]
fn a_malformed_expression_is_a_parse_error() {
    for expression in [
        r#"fee in 1.."2""#,
        "ok in false..true",
        "fee in 2..1",
        "a exists",
    ] {
        assert!(expression.parse::<Filter>().is_err(), "{expression}");
    }
}

#[test]
fn a_long_path_does_not_use_up_the_stack() {
    let expression = format!("a{} = 1", "[0]".repeat(100_000));

    assert!(!matches(&expression, &json!({"a": [[1]]})));
}
