//! Printing fixed responses, as the service sends them.

use burgess_api::Page;
use burgess_core::flow::{FlowId, TreeId};

use super::*;

const TREE: &str = "019a5c2e7f00a1b2c3d4e5f6";

/// A page as the service sends it: a state with meta, a split, a state
/// whose payload spans lines, and a state whose payload was not embedded.
fn page() -> Page {
    serde_json::from_str(&format!(
        r#"{{"rows": [
            {{"flow_id": "{TREE}00000000", "tree": "{TREE}", "subflow": 0,
              "ts": 1793880000000, "kind": "state", "service": "checkout",
              "category": "order", "meta": {{"region": "eu", "note": "two words"}},
              "payload_type": "json", "payload": {{"order": 7}}, "child_subflow": null}},
            {{"flow_id": "{TREE}00000000", "tree": "{TREE}", "subflow": 0,
              "ts": 1793880000001, "kind": "split", "service": "checkout",
              "category": "payment", "meta": {{}},
              "payload_type": null, "payload": null, "child_subflow": 1059757057}},
            {{"flow_id": "{TREE}3f2a9c01", "tree": "{TREE}", "subflow": 1059757057,
              "ts": 1793880000004, "kind": "state", "service": "checkout",
              "category": "charge", "meta": {{}},
              "payload_type": "json", "payload": {{
                "amount": 42,
                "currency": "EUR"
              }}, "child_subflow": null}},
            {{"flow_id": "{TREE}3f2a9c01", "tree": "{TREE}", "subflow": 1059757057,
              "ts": 1793880000005, "kind": "state", "service": "checkout",
              "category": "charge", "meta": {{}},
              "payload_type": "json", "payload": null, "child_subflow": null}}
        ]}}"#
    ))
    .unwrap()
}

fn tree() -> TreeId {
    TREE.parse().unwrap()
}

fn printed(output: Output, heading: Heading, page: &Page) -> String {
    let mut out = Vec::new();
    output.write_rows(heading, page, &mut out).unwrap();
    String::from_utf8(out).unwrap()
}

fn notes(page: &Page) -> String {
    let mut err = Vec::new();
    write_notes(page, &mut err).unwrap();
    String::from_utf8(err).unwrap()
}

#[test]
fn a_listing_has_one_aligned_line_per_row() {
    assert_eq!(
        printed(Output::Listing, Heading::Tree(tree()), &page()),
        format!(
            "tree {TREE}\n\
             \n\
             2026-11-05T12:00:00.000Z  00000000  checkout/order    state  note=\"two words\" region=eu {{\"order\":7}}\n\
             2026-11-05T12:00:00.001Z  00000000  checkout/payment  split  -> 3f2a9c01\n\
             2026-11-05T12:00:00.004Z  3f2a9c01  checkout/charge   state  {{\"amount\":42,\"currency\":\"EUR\"}}\n\
             2026-11-05T12:00:00.005Z  3f2a9c01  checkout/charge   state  -\n\
             \n\
             4 rows\n"
        )
    );
}

#[test]
fn a_listing_of_a_flow_names_the_flow() {
    let flow: FlowId = format!("{TREE}3f2a9c01").parse().unwrap();
    let listing = printed(Output::Listing, Heading::Flow(flow), &page());
    assert!(
        listing.starts_with(&format!("flow {TREE}3f2a9c01\n\n")),
        "{listing}"
    );
}

#[test]
fn an_empty_listing_says_so() {
    let empty = Page {
        rows: Vec::new(),
        cursor: None,
        problems: Vec::new(),
    };
    assert_eq!(
        printed(Output::Listing, Heading::Tree(tree()), &empty),
        format!("tree {TREE}\n\nno rows\n")
    );
}

#[test]
fn json_lines_are_the_api_rows_one_per_line() {
    let page = page();
    let printed = printed(Output::JsonLines, Heading::Tree(tree()), &page);

    let lines: Vec<&str> = printed.lines().collect();
    assert_eq!(lines.len(), page.rows.len());
    assert_eq!(
        lines[2],
        format!(
            r#"{{"flow_id":"{TREE}3f2a9c01","tree":"{TREE}","subflow":1059757057,"ts":1793880000004,"kind":"state","service":"checkout","category":"charge","meta":{{}},"payload_type":"json","payload":{{"amount":42,"currency":"EUR"}},"child_subflow":null}}"#
        )
    );
    for (line, row) in lines.iter().zip(&page.rows) {
        let read: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(read, serde_json::to_value(row).unwrap());
    }
}

#[test]
fn control_characters_from_producers_are_escaped() {
    let mut page = page();
    page.rows.truncate(1);
    let row = &mut page.rows[0];
    row.service = "check\u{1b}]0;x\u{7}out".into();
    row.category = "two\nlines".into();
    row.meta = [("k\u{1b}".to_string(), "v\u{1b}[2K".to_string())].into();

    let listing = printed(Output::Listing, Heading::Tree(tree()), &page);

    assert!(!listing.contains(['\u{1b}', '\u{7}']), "{listing:?}");
    assert_eq!(listing.lines().count(), 5, "{listing}");
    assert!(
        listing.contains(
            r#"check\u{1b}]0;x\u{7}out/two\nlines  state  k\u{1b}="v\u{1b}[2K" {"order":7}"#
        ),
        "{listing}"
    );
}

#[test]
fn columns_line_up_whatever_the_alphabet() {
    let mut page = page();
    page.rows.truncate(2);
    page.rows[0].service = "заказ".into();

    let listing = printed(Output::Listing, Heading::Tree(tree()), &page);

    let kind_columns: Vec<usize> = listing
        .lines()
        .filter_map(|line| {
            let kind = line.find("  state  ").or_else(|| line.find("  split  "))?;
            Some(line[..kind].chars().count())
        })
        .collect();
    assert_eq!(kind_columns.len(), 2, "{listing}");
    assert_eq!(kind_columns[0], kind_columns[1], "{listing}");
}

#[test]
fn a_complete_page_has_no_notes() {
    assert_eq!(notes(&page()), "");
}

#[test]
fn problems_and_a_cut_page_are_noted() {
    let page = Page {
        cursor: Some("NEXT".into()),
        problems: vec!["the manifest of 20261105 is invalid: bad frame".into()],
        ..page()
    };
    assert_eq!(
        notes(&page),
        "problem: the manifest of 20261105 is invalid: bad frame\n\
         the service could not read everything; rows or payloads may be missing\n\
         more rows exist than the 4 shown; a larger --limit, up to the service's maximum, shows more\n"
    );
}
