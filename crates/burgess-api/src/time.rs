//! Times as requests give them.

use jiff::Timestamp;

/// Why a text is not a time a request can give.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{found:?} is not an RFC 3339 time or unix milliseconds")]
pub struct TimeParseError {
    found: String,
}

/// A time given as unix milliseconds, as the API returns times, or as RFC
/// 3339, as people type them.
///
/// The service and the CLI both read times with this, so a time the CLI
/// accepts is one the service accepts, and a bad one is reported in the
/// same words.
pub fn parse_time(text: &str) -> Result<Timestamp, TimeParseError> {
    let parsed = if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
        text.parse()
            .ok()
            .and_then(|millis| Timestamp::from_millisecond(millis).ok())
    } else {
        text.parse().ok()
    };
    parsed.ok_or_else(|| TimeParseError {
        found: text.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_unix_milliseconds_and_rfc_3339() {
        let expected = Timestamp::from_millisecond(1_793_880_000_000).unwrap();
        for text in [
            "1793880000000",
            "2026-11-05T12:00:00Z",
            "2026-11-05T13:00:00+01:00",
        ] {
            assert_eq!(parse_time(text), Ok(expected), "{text}");
        }
    }

    #[test]
    fn refuses_anything_else_in_one_message() {
        for text in ["", "yesterday", "2026-11-05", "99999999999999999999"] {
            assert_eq!(
                parse_time(text).unwrap_err().to_string(),
                format!("{text:?} is not an RFC 3339 time or unix milliseconds")
            );
        }
    }
}
