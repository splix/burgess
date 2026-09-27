//! The hex text form shared by tree ids and flow ids.
//!
//! Ids are typed by people and pasted between tools, so the parser accepts any
//! case but nothing else. Rejecting dashes and `0x` keeps a single spelling per
//! id, which is what makes an id safe to grep for in logs.

use std::fmt;

/// Which text form a parser expected, so an error can say what would have been
/// accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdForm {
    /// 24 hex characters.
    Tree,
    /// 32 hex characters.
    Flow,
    /// Either of the above.
    TreeOrFlow,
}

impl fmt::Display for IdForm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdForm::Tree => f.write_str("24 hex characters (a tree id)"),
            IdForm::Flow => f.write_str("32 hex characters (a flow id)"),
            IdForm::TreeOrFlow => f.write_str("24 or 32 hex characters (a tree or a flow id)"),
        }
    }
}

/// Why a string is not a valid id.
///
/// The messages are shown to investigators as they are, by the CLI and in API
/// error responses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdParseError {
    /// A character other than `0-9`, `a-f` or `A-F`.
    #[error("invalid character {found:?} at position {position}, an id contains only hex digits")]
    NotHex {
        /// Zero-based character position.
        position: usize,
        /// The offending character.
        found: char,
    },
    /// Only hex digits, but not as many as the expected form needs.
    #[error("expected {expected}, found {found}")]
    WrongLength {
        /// The form the parser was asked for.
        expected: IdForm,
        /// Number of hex digits in the input.
        found: usize,
    },
}

/// Most hex digits any id has: the 32 of a flow id.
const MAX_DIGITS: usize = 32;

/// Hex digits of an id, validated but not yet sized.
///
/// Checking the characters before the length gives the more helpful error for
/// the common mistakes: a UUID-style dash or a `0x` prefix is reported as the
/// character it is, not as a length that is off by a few.
///
/// The input comes from API requests and command lines, so it can be any size.
/// Only the first [`MAX_DIGITS`] digits are kept; the rest are counted, so an
/// oversized input costs no allocation and still gets an exact length in its
/// error.
pub(super) struct HexDigits {
    digits: [u8; MAX_DIGITS],
    count: usize,
}

impl HexDigits {
    pub(super) fn parse(text: &str) -> Result<Self, IdParseError> {
        let mut parsed = HexDigits {
            digits: [0; MAX_DIGITS],
            count: 0,
        };
        for (position, found) in text.chars().enumerate() {
            let digit = found
                .to_digit(16)
                .ok_or(IdParseError::NotHex { position, found })?;
            if let Some(slot) = parsed.digits.get_mut(parsed.count) {
                *slot = digit as u8;
            }
            parsed.count += 1;
        }
        Ok(parsed)
    }

    /// Packs the digits into `N` bytes, or `None` when there are not exactly
    /// `2 * N` of them.
    ///
    /// This is the only place that compares the length, so each id form
    /// states its size once, as the byte array it asks for.
    pub(super) fn to_bytes<const N: usize>(&self) -> Option<[u8; N]> {
        (self.count == N * 2 && self.count <= MAX_DIGITS)
            .then(|| std::array::from_fn(|i| (self.digits[2 * i] << 4) | self.digits[2 * i + 1]))
    }

    /// The error for digits that fit none of the sizes `expected` allows.
    pub(super) fn wrong_length(&self, expected: IdForm) -> IdParseError {
        IdParseError::WrongLength {
            expected,
            found: self.count,
        }
    }
}

pub(super) fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    bytes.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_any_case() {
        let bytes = HexDigits::parse("aBcD").unwrap().to_bytes::<2>();
        assert_eq!(bytes, Some([0xab, 0xcd]));
    }

    #[test]
    fn reports_first_non_hex_character() {
        let error = HexDigits::parse("0x12").err().unwrap();
        assert_eq!(
            error,
            IdParseError::NotHex {
                position: 1,
                found: 'x'
            }
        );
    }

    #[test]
    fn packs_only_the_exact_length() {
        let digits = HexDigits::parse("abc").unwrap();
        assert_eq!(digits.to_bytes::<1>(), None);
        assert_eq!(digits.to_bytes::<2>(), None);
    }

    #[test]
    fn counts_digits_beyond_the_longest_id() {
        let digits = HexDigits::parse(&"a".repeat(1_000)).unwrap();
        assert_eq!(digits.to_bytes::<16>(), None);
        assert_eq!(
            digits.wrong_length(IdForm::Flow),
            IdParseError::WrongLength {
                expected: IdForm::Flow,
                found: 1_000
            }
        );
    }

    #[test]
    fn error_message_names_the_expected_form() {
        let error = IdParseError::WrongLength {
            expected: IdForm::Flow,
            found: 30,
        };
        assert_eq!(
            error.to_string(),
            "expected 32 hex characters (a flow id), found 30"
        );
    }
}
