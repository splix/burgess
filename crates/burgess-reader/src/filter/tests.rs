//! What the parser reads from an expression, and what it refuses.

use std::num::NonZeroUsize;

use serde_json::Number;

use super::*;

fn parse(text: &str) -> Filter {
    text.parse()
        .unwrap_or_else(|error| panic!("{text:?}: {error}"))
}

fn error(text: &str) -> FilterError {
    match text.parse::<Filter>() {
        Ok(filter) => panic!("{text:?} parsed as {filter:?}"),
        Err(error) => error,
    }
}

fn kind(text: &str) -> FilterErrorKind {
    error(text).kind
}

fn key(key: &str) -> Step {
    Step::Key(key.into())
}

fn path(steps: Vec<Step>) -> Path {
    Path(steps)
}

fn number(text: &str) -> Number {
    text.parse().unwrap()
}

fn compare(path: Path, comparison: Comparison, scalar: Scalar) -> Filter {
    Filter {
        path,
        test: Test::Compare(comparison, scalar),
    }
}

#[test]
fn every_example_of_the_grammar_parses() {
    let examples = [
        (
            "amount > 100.0",
            compare(
                path(vec![key("amount")]),
                Comparison::Gt,
                Scalar::Number(number("100.0")),
            ),
        ),
        (
            "tx.inputs[].value >= 1000",
            compare(
                path(vec![key("tx"), key("inputs"), Step::Any, key("value")]),
                Comparison::Ge,
                Scalar::Number(number("1000")),
            ),
        ),
        (
            r#"tx.outputs[-1].address = "0xabc""#,
            compare(
                path(vec![
                    key("tx"),
                    key("outputs"),
                    Step::Index(Index::FromEnd(NonZeroUsize::MIN)),
                    key("address"),
                ]),
                Comparison::Eq,
                Scalar::String("0xabc".into()),
            ),
        ),
        (
            r#"headers["content-type"] != "application/json""#,
            compare(
                path(vec![key("headers"), key("content-type")]),
                Comparison::Ne,
                Scalar::String("application/json".into()),
            ),
        ),
        (
            "fee in 10..=20",
            Filter {
                path: path(vec![key("fee")]),
                test: Test::Range {
                    bounds: RangeBounds::Numbers(number("10"), number("20")),
                    end: RangeEnd::Inclusive,
                },
            },
        ),
        (
            "error = null",
            compare(path(vec![key("error")]), Comparison::Eq, Scalar::Null),
        ),
        (
            r#". = "timeout""#,
            compare(
                Path::default(),
                Comparison::Eq,
                Scalar::String("timeout".into()),
            ),
        ),
        (
            "[0].code = 500",
            compare(
                path(vec![Step::Index(Index::FromStart(0)), key("code")]),
                Comparison::Eq,
                Scalar::Number(number("500")),
            ),
        ),
    ];
    for (text, expected) in examples {
        assert_eq!(parse(text), expected, "{text}");
    }
}

#[test]
fn each_operator_is_read_with_or_without_spaces() {
    let operators = [
        ("=", Comparison::Eq),
        ("!=", Comparison::Ne),
        ("<", Comparison::Lt),
        ("<=", Comparison::Le),
        (">", Comparison::Gt),
        (">=", Comparison::Ge),
    ];
    for (operator, comparison) in operators {
        for text in [format!("a{operator}1"), format!("a  {operator}\t1")] {
            assert_eq!(
                parse(&text).test,
                Test::Compare(comparison, Scalar::Number(number("1"))),
                "{text}"
            );
        }
    }
}

#[test]
fn a_range_without_its_end_excludes_it() {
    assert_eq!(
        parse(r#"name in "a" .. "b""#).test,
        Test::Range {
            bounds: RangeBounds::Strings("a".into(), "b".into()),
            end: RangeEnd::Exclusive,
        }
    );
    assert_eq!(
        parse("fee in -1.5..2e3").test,
        Test::Range {
            bounds: RangeBounds::Numbers(number("-1.5"), number("2e3")),
            end: RangeEnd::Exclusive,
        }
    );
}

#[test]
fn range_bounds_of_different_types_are_refused() {
    assert_eq!(
        kind(r#"fee in 1.."2""#),
        FilterErrorKind::MixedRange {
            start: ScalarType::Number,
            end: ScalarType::String,
        }
    );
    assert_eq!(
        kind("fee in null..=1"),
        FilterErrorKind::MixedRange {
            start: ScalarType::Null,
            end: ScalarType::Number,
        }
    );
}

#[test]
fn range_bounds_without_an_order_are_refused() {
    assert_eq!(
        kind("ok in false..true"),
        FilterErrorKind::UnorderedRange(ScalarType::Bool)
    );
    assert_eq!(
        kind("gone in null..null"),
        FilterErrorKind::UnorderedRange(ScalarType::Null)
    );
}

#[test]
fn a_range_that_holds_no_value_is_refused() {
    for text in [
        "fee in 20..10",
        "fee in 10..10",
        "fee in 1.0..1",
        "fee in 20..=10",
        r#"name in "b".."a""#,
        r#"name in "a".."a""#,
    ] {
        assert_eq!(kind(text), FilterErrorKind::EmptyRange, "{text}");
    }
    for text in [
        "fee in 10..=10",
        "fee in -0..=0",
        "fee in 1..1.5",
        r#"name in "a"..="a""#,
    ] {
        parse(text);
    }
}

#[test]
fn keywords_are_lower_case_only() {
    assert_eq!(kind("fee IN 1..2"), FilterErrorKind::ExpectedOperator);
    assert_eq!(kind("fee In 1..2"), FilterErrorKind::ExpectedOperator);
    for literal in ["TRUE", "False", "NULL", "Null"] {
        assert_eq!(
            kind(&format!("a = {literal}")),
            FilterErrorKind::ExpectedScalar,
            "{literal}"
        );
    }
    assert_eq!(
        parse("a = true").test,
        Test::Compare(Comparison::Eq, Scalar::Bool(true))
    );
    assert_eq!(
        parse("a = false").test,
        Test::Compare(Comparison::Eq, Scalar::Bool(false))
    );
}

#[test]
fn in_needs_a_space_on_each_side() {
    assert_eq!(kind(r#"fee in"a".."b""#), FilterErrorKind::ExpectedSpace);
    assert_eq!(kind("fee in"), FilterErrorKind::ExpectedSpace);
    // A longer word is not `in`, thus the operator is missing.
    assert_eq!(kind("fee in1..2"), FilterErrorKind::ExpectedOperator);
    assert_eq!(kind("fee inx 1..2"), FilterErrorKind::ExpectedOperator);
    // Without a space before it, `in` is part of the key.
    assert_eq!(kind("feein 1..2"), FilterErrorKind::ExpectedOperator);
}

#[test]
fn a_key_can_have_dashes_digits_and_be_a_keyword() {
    assert_eq!(parse("a-1 = 1").path, path(vec![key("a-1")]));
    assert_eq!(parse("2fa = true").path, path(vec![key("2fa")]));
    assert_eq!(
        parse("in in 1..2").path,
        path(vec![key("in")]),
        "the path is read before the operator"
    );
    assert_eq!(parse("null = 1").path, path(vec![key("null")]));
}

#[test]
fn a_quoted_key_is_a_json_string() {
    assert_eq!(
        parse(r#"["a.b"]["\"q\""]["é"] = 1"#).path,
        path(vec![key("a.b"), key("\"q\""), key("é")])
    );
    assert!(matches!(
        kind(r#"["\x"] = 1"#),
        FilterErrorKind::InvalidString(_)
    ));
}

#[test]
fn indexes_count_from_the_start_or_from_the_end() {
    assert_eq!(
        parse("a[3][-2] = 1").path,
        path(vec![
            key("a"),
            Step::Index(Index::FromStart(3)),
            Step::Index(Index::FromEnd(NonZeroUsize::new(2).unwrap())),
        ])
    );
    assert_eq!(kind("a[-0] = 1"), FilterErrorKind::NegativeZero);
    assert_eq!(
        kind("a[99999999999999999999999] = 1"),
        FilterErrorKind::IndexTooLarge
    );
}

#[test]
fn a_path_that_breaks_the_grammar_is_refused() {
    let cases = [
        ("", FilterErrorKind::ExpectedPath),
        ("= 1", FilterErrorKind::ExpectedPath),
        (".a = 1", FilterErrorKind::StepAfterRoot),
        (".[0] = 1", FilterErrorKind::StepAfterRoot),
        ("a. = 1", FilterErrorKind::ExpectedKey),
        ("a..b = 1", FilterErrorKind::ExpectedKey),
        ("a[ = 1", FilterErrorKind::ExpectedSubscript),
        ("a[1 = 1", FilterErrorKind::ExpectedSubscript),
        ("a[-] = 1", FilterErrorKind::ExpectedSubscript),
        ("a[x] = 1", FilterErrorKind::ExpectedSubscript),
        ("a b = 1", FilterErrorKind::ExpectedOperator),
        ("a == 1", FilterErrorKind::ExpectedScalar),
    ];
    for (text, expected) in cases {
        assert_eq!(kind(text), expected, "{text:?}");
    }
}

#[test]
fn scalars_follow_the_json_grammar() {
    for text in ["a = 01", "a = -", "a = 1e", "a = -01.5"] {
        assert!(
            matches!(kind(text), FilterErrorKind::InvalidNumber(_)),
            "{text}"
        );
    }
    for text in ["a = +1", "a = .5", "a = 'x'", "a = x"] {
        assert_eq!(kind(text), FilterErrorKind::ExpectedScalar, "{text}");
    }
    assert_eq!(kind("a = 1."), FilterErrorKind::TrailingText);
    let invalid = error("a = 01");
    assert_eq!(invalid.column, 5);
    assert!(
        !invalid.to_string().contains("line"),
        "one position only: {invalid}"
    );
    assert_eq!(kind(r#"a = "open"#), FilterErrorKind::UnclosedString);
    assert_eq!(kind(r#"a = "\"#), FilterErrorKind::UnclosedString);
}

#[test]
fn an_integer_stays_exact() {
    let Test::Compare(_, Scalar::Number(big)) = parse("a = 9007199254740993").test else {
        panic!("a number expected");
    };
    assert_eq!(big.as_u64(), Some(9_007_199_254_740_993));
    let Test::Compare(_, Scalar::Number(negative)) = parse("a = -5").test else {
        panic!("a number expected");
    };
    assert_eq!(negative.as_i64(), Some(-5));
}

#[test]
fn nothing_can_follow_the_filter() {
    assert_eq!(kind("a = truex"), FilterErrorKind::TrailingText);
    assert_eq!(kind("a = 1 2"), FilterErrorKind::TrailingText);
    assert_eq!(kind("fee in 1..2..3"), FilterErrorKind::TrailingText);
    assert_eq!(kind("fee in 1 2"), FilterErrorKind::ExpectedRange);
}

#[test]
fn spaces_around_the_expression_are_ignored() {
    assert_eq!(parse("  a = 1\n"), parse("a = 1"));
}

#[test]
fn an_error_tells_the_column_in_characters() {
    let literal = error(r#"["é"] = TRUE"#);
    assert_eq!(literal.kind, FilterErrorKind::ExpectedScalar);
    assert_eq!(literal.column, 9);
    assert_eq!(
        literal.to_string(),
        "expected a string, a number, `true`, `false` or `null` at column 9"
    );
    assert_eq!(
        error(r#"fee in 1.."2""#).column,
        8,
        "at the start of the range"
    );
}
