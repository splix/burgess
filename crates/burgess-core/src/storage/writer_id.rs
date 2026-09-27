//! The name a writer deployment stores its checkpoint and manifests under.

use std::fmt;
use std::str::FromStr;

/// Identifies a writer deployment. It becomes part of an object name, so it
/// is limited to characters that are safe in any object store key and cannot
/// add path segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterId(String);

/// Why a string cannot be a writer id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "{found:?} is not a writer id: use 1 to 64 ASCII letters, digits, '.', '_' or '-', not only dots"
)]
pub struct WriterIdError {
    /// The rejected text.
    pub found: String,
}

impl FromStr for WriterId {
    type Err = WriterIdError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');
        let valid = (1..=64).contains(&text.len())
            && text.chars().all(allowed)
            && !text.chars().all(|c| c == '.');
        if !valid {
            return Err(WriterIdError {
                found: text.to_string(),
            });
        }
        Ok(WriterId(text.to_string()))
    }
}

impl fmt::Display for WriterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plain_names() {
        for text in ["writer", "writer-1", "prod_eu.1"] {
            assert_eq!(text.parse::<WriterId>().unwrap().to_string(), text);
        }
    }

    #[test]
    fn rejects_names_that_could_change_the_path() {
        let too_long = "w".repeat(65);
        for text in ["", ".", "..", "a/b", "a b", "writé", too_long.as_str()] {
            assert!(text.parse::<WriterId>().is_err(), "{text:?}");
        }
    }
}
