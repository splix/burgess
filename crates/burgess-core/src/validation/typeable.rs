//! Which characters an investigator can type back into a query.
//!
//! A `domain_id` is looked up by exact comparison with what an investigator
//! types or pastes. Characters that are invisible, look like another
//! character, or render differently from one screen to the next make a stored
//! id impossible to find, so they are rejected before the id is stored.

use unicode_properties::{GeneralCategory, UnicodeEmoji, UnicodeGeneralCategory};

/// Whether `c` may appear in a `domain_id`.
///
/// Allowed are letters, combining marks, numbers, punctuation, symbols and
/// the plain space. That leaves out control and formatting characters, line
/// and paragraph separators, other kinds of spaces, enclosing marks, and
/// private-use and unassigned code points. Emoji and the parts emoji sequences
/// are built from, such as skin-tone modifiers and the emoji variation
/// selector, are rejected by their emoji property whatever their category;
/// ASCII digits, `#` and `*` carry that property too and stay allowed.
pub(super) fn is_typeable(c: char) -> bool {
    if c == ' ' {
        return true;
    }
    if !c.is_ascii() && c.is_emoji_char_or_emoji_component() {
        return false;
    }
    matches!(
        c.general_category(),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
            | GeneralCategory::NonspacingMark
            | GeneralCategory::SpacingMark
            | GeneralCategory::DecimalNumber
            | GeneralCategory::LetterNumber
            | GeneralCategory::OtherNumber
            | GeneralCategory::ConnectorPunctuation
            | GeneralCategory::DashPunctuation
            | GeneralCategory::OpenPunctuation
            | GeneralCategory::ClosePunctuation
            | GeneralCategory::InitialPunctuation
            | GeneralCategory::FinalPunctuation
            | GeneralCategory::OtherPunctuation
            | GeneralCategory::MathSymbol
            | GeneralCategory::CurrencySymbol
            | GeneralCategory::ModifierSymbol
            | GeneralCategory::OtherSymbol
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rejected(text: &str) -> Vec<char> {
        text.chars().filter(|c| !is_typeable(*c)).collect()
    }

    #[test]
    fn accepts_text_people_type() {
        for text in [
            "ORD-1001",
            "Jane Doe",
            "a/b?c=d&e#f*1",
            "заказ №7",
            "注文-42",
            "café",
            "e\u{301}", // e followed by a combining acute accent
            "€5 + 3 = ¥8",
            "25°C ✓",
            "user@example.com",
        ] {
            assert_eq!(rejected(text), [], "text {text:?}");
        }
    }

    #[test]
    fn rejects_controls() {
        assert_eq!(
            rejected("\0\t\n\r\u{1b}\u{7f}\u{85}"),
            ['\0', '\t', '\n', '\r', '\u{1b}', '\u{7f}', '\u{85}']
        );
    }

    #[test]
    fn rejects_invisible_formatting() {
        let invisible = [
            '\u{ad}',   // soft hyphen
            '\u{200b}', // zero-width space
            '\u{200d}', // zero-width joiner
            '\u{200e}', // left-to-right mark
            '\u{202e}', // right-to-left override
            '\u{2060}', // word joiner
            '\u{feff}', // byte order mark
        ];
        for c in invisible {
            assert!(!is_typeable(c), "character {c:?}");
        }
    }

    #[test]
    fn rejects_separators_other_than_plain_space() {
        for c in ['\u{a0}', '\u{2003}', '\u{3000}', '\u{2028}', '\u{2029}'] {
            assert!(!is_typeable(c), "character {c:?}");
        }
    }

    #[test]
    fn rejects_emoji_and_their_parts() {
        let emoji = [
            '😀',        // plain emoji
            '🇩',         // regional indicator, half of a flag
            '\u{1f3fd}', // skin-tone modifier
            '\u{fe0f}',  // emoji variation selector
            '\u{20e3}',  // combining keycap
            '©',
            '‼',
        ];
        for c in emoji {
            assert!(!is_typeable(c), "character {c:?}");
        }
    }

    #[test]
    fn rejects_private_use_and_unassigned() {
        for c in ['\u{e000}', '\u{f8ff}', '\u{10ffff}', '\u{fffe}'] {
            assert!(!is_typeable(c), "character {c:?}");
        }
    }
}
