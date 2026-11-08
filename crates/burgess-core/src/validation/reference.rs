//! Rules for references, the links from outside ids to flows.
//!
//! An investigator finds a flow by typing an id they know, so a stored
//! reference must match what they type: lookups compare `ref_type` and
//! `domain_id` exactly. Surrounding whitespace is the one difference nobody
//! types on purpose, so it is trimmed; case and everything else are kept.

use std::fmt;

use super::error::ValidationError;
use super::typeable::is_typeable;
use super::{Limits, Validate};
use crate::wire::Reference;

/// A reference field with rules of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefField {
    /// `Reference.ref_type`: the kind of id, like `order_id`.
    RefType,
    /// `Reference.domain_id`: the id itself, as the application knows it.
    DomainId,
}

impl RefField {
    /// Longest value allowed, in UTF-8 bytes after trimming.
    ///
    /// Bytes rather than characters, so the limit bounds storage whatever
    /// script the value is written in.
    pub fn max_bytes(self) -> usize {
        match self {
            RefField::RefType => 64,
            RefField::DomainId => 256,
        }
    }

    /// A `ref_type` is typed in queries and URLs, so it is kept to characters
    /// that need no quoting anywhere. A `domain_id` may be any text that can
    /// be typed back.
    fn allows(self, c: char) -> bool {
        match self {
            RefField::RefType => c.is_ascii_alphanumeric() || c == '_' || c == '-',
            RefField::DomainId => is_typeable(c),
        }
    }

    /// Trims `value` in place, then checks it is not empty, has only allowed
    /// characters and fits the limit.
    ///
    /// Characters are checked before the length, so a value that breaks both
    /// rules is reported for the one that cannot be fixed by shortening it.
    ///
    /// Lookups normalize the value they search for with the same rules, thus
    /// a query finds what the writer stored, and a value that the writer
    /// never stores is refused with the same words.
    pub fn normalize(self, value: &mut String) -> Result<(), ValidationError> {
        trim_in_place(value);
        if value.is_empty() {
            return Err(ValidationError::Empty { field: self });
        }
        if let Some((position, found)) = value.chars().enumerate().find(|(_, c)| !self.allows(*c)) {
            return Err(ValidationError::Character {
                field: self,
                position,
                found,
            });
        }
        if value.len() > self.max_bytes() {
            return Err(ValidationError::TooLong {
                field: self,
                found: value.len(),
                max: self.max_bytes(),
            });
        }
        Ok(())
    }
}

impl fmt::Display for RefField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RefField::RefType => f.write_str("ref_type"),
            RefField::DomainId => f.write_str("domain_id"),
        }
    }
}

/// `service` and `category` have no rules yet and are stored as sent.
impl Validate for Reference {
    fn validate(&mut self, _limits: &Limits) -> Result<(), ValidationError> {
        RefField::RefType.normalize(&mut self.ref_type)?;
        RefField::DomainId.normalize(&mut self.domain_id)
    }
}

/// Keeps the string's buffer, since most values need no trimming at all and
/// the rest lose only a few bytes.
fn trim_in_place(value: &mut String) {
    value.truncate(value.trim_end().len());
    let start = value.len() - value.trim_start().len();
    value.drain(..start);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate(ref_type: &str, domain_id: &str) -> Result<Reference, ValidationError> {
        let mut reference = Reference {
            ref_type: ref_type.into(),
            domain_id: domain_id.into(),
            service: "orders".into(),
            category: None,
        };
        reference.validate(&Limits::default())?;
        Ok(reference)
    }

    fn repeat(c: char, bytes: usize) -> String {
        c.to_string().repeat(bytes / c.len_utf8())
    }

    #[test]
    fn valid_reference_is_unchanged() {
        let valid = validate("order_id", "ORD-1001").unwrap();
        assert_eq!(
            (valid.ref_type.as_str(), valid.domain_id.as_str()),
            ("order_id", "ORD-1001")
        );
    }

    #[test]
    fn trims_both_fields() {
        let valid = validate(" \torder_id\n", "\u{3000} ORD-1001 ").unwrap();
        assert_eq!(valid.ref_type, "order_id");
        assert_eq!(valid.domain_id, "ORD-1001");
    }

    #[test]
    fn preserves_case() {
        let valid = validate("Order_ID", "ord-AbC").unwrap();
        assert_eq!(valid.ref_type, "Order_ID");
        assert_eq!(valid.domain_id, "ord-AbC");
    }

    #[test]
    fn keeps_inner_spaces_of_domain_id() {
        assert_eq!(
            validate("name", "Jane  Doe").unwrap().domain_id,
            "Jane  Doe"
        );
    }

    #[test]
    fn rejects_fields_empty_after_trimming() {
        assert_eq!(
            validate("  ", "ORD-1").unwrap_err(),
            ValidationError::Empty {
                field: RefField::RefType
            }
        );
        assert_eq!(
            validate("order_id", "\t\n").unwrap_err(),
            ValidationError::Empty {
                field: RefField::DomainId
            }
        );
    }

    #[test]
    fn ref_type_length_boundary() {
        assert!(validate(&repeat('a', 64), "ORD-1").is_ok());
        assert_eq!(
            validate(&repeat('a', 65), "ORD-1").unwrap_err(),
            ValidationError::TooLong {
                field: RefField::RefType,
                found: 65,
                max: 64
            }
        );
    }

    #[test]
    fn domain_id_length_boundary() {
        assert!(validate("order_id", &repeat('x', 256)).is_ok());
        assert_eq!(
            validate("order_id", &repeat('x', 257)).unwrap_err(),
            ValidationError::TooLong {
                field: RefField::DomainId,
                found: 257,
                max: 256
            }
        );
    }

    #[test]
    fn domain_id_length_counts_bytes_not_characters() {
        // 'é' is two bytes in UTF-8, so 129 of them are 258 bytes.
        assert!(validate("name", &repeat('é', 256)).is_ok());
        assert_eq!(
            validate("name", &"é".repeat(129)).unwrap_err(),
            ValidationError::TooLong {
                field: RefField::DomainId,
                found: 258,
                max: 256
            }
        );
    }

    #[test]
    fn length_is_measured_after_trimming() {
        let padded = format!("  {}  ", repeat('a', 64));
        assert!(validate(&padded, "ORD-1").is_ok());
    }

    #[test]
    fn ref_type_allows_letters_digits_underscore_and_dash() {
        assert!(validate("AZaz09_-", "ORD-1").is_ok());
    }

    #[test]
    fn ref_type_rejects_other_characters() {
        for (ref_type, position, found) in [
            ("order id", 5, ' '),
            ("order.id", 5, '.'),
            ("ordér", 3, 'é'),
            ("order:id", 5, ':'),
        ] {
            assert_eq!(
                validate(ref_type, "ORD-1").unwrap_err(),
                ValidationError::Character {
                    field: RefField::RefType,
                    position,
                    found
                },
                "ref_type {ref_type:?}"
            );
        }
    }

    #[test]
    fn long_non_ascii_ref_type_is_reported_for_its_characters() {
        let error = validate(&"заказ".repeat(20), "ORD-1").unwrap_err();
        assert!(
            matches!(error, ValidationError::Character { position: 0, .. }),
            "{error:?}"
        );
    }

    #[test]
    fn domain_id_rejects_untypeable_characters() {
        for found in ['\n', '\u{200b}', '\u{feff}', '😀'] {
            assert_eq!(
                validate("id", &format!("ORD{found}1")).unwrap_err(),
                ValidationError::Character {
                    field: RefField::DomainId,
                    position: 3,
                    found
                },
                "character {found:?}"
            );
        }
    }

    #[test]
    fn trims_only_whitespace() {
        let mut value = " \u{feff}ORD-1 ".to_string();
        trim_in_place(&mut value);
        assert_eq!(value, "\u{feff}ORD-1");
    }

    #[test]
    fn trims_whitespace_only_value_to_empty() {
        let mut value = " \t ".to_string();
        trim_in_place(&mut value);
        assert_eq!(value, "");
    }

    #[test]
    fn error_message_names_field_and_limit() {
        let error = validate(&repeat('a', 70), "ORD-1").unwrap_err();
        assert_eq!(
            error.to_string(),
            "ref_type is 70 bytes long, at most 64 are allowed"
        );
    }
}
