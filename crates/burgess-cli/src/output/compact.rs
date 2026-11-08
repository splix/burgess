//! JSON on one line.

/// `json` without the whitespace between its tokens.
///
/// A payload is embedded as the producer wrote it, which may span lines,
/// while a listing or JSON lines needs each row on one line. Removing only
/// the whitespace outside strings keeps every value exactly as written,
/// where parsing and printing again could change how numbers read.
pub(super) fn compact(json: &str) -> String {
    let mut compact = String::with_capacity(json.len());
    let mut in_string = false;
    let mut escaped = false;
    for c in json.chars() {
        if in_string {
            compact.push(c);
            match c {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
        } else if !c.is_ascii_whitespace() {
            in_string = c == '"';
            compact.push(c);
        }
    }
    compact
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_whitespace_between_tokens() {
        assert_eq!(
            compact("{\n  \"a\": [1, 2],\r\n\t\"b\" : null\n}"),
            r#"{"a":[1,2],"b":null}"#
        );
    }

    #[test]
    fn keeps_strings_as_written() {
        let json = r#"{"text": "two  spaces, a \" quote and \\", "n": 1.50}"#;
        assert_eq!(
            compact(json),
            r#"{"text":"two  spaces, a \" quote and \\","n":1.50}"#
        );
    }
}
