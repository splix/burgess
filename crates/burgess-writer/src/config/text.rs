//! Reading text settings that the environment may deliver as numbers.
//!
//! The environment has only strings, and figment turns those that look like
//! numbers or booleans into numbers and booleans. A writer id, topic, bucket
//! or password may well look like a number, and must still be read as text.

use std::collections::BTreeMap;
use std::fmt;

use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

/// Any scalar, read as its text.
struct Text(String);

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(TextVisitor).map(Text)
    }
}

struct TextVisitor;

impl Visitor<'_> for TextVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("text")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
        Ok(value.to_string())
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<String, E> {
        Ok(value)
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<String, E> {
        Ok(value.to_string())
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<String, E> {
        Ok(value.to_string())
    }

    fn visit_i128<E: de::Error>(self, value: i128) -> Result<String, E> {
        Ok(value.to_string())
    }

    fn visit_u128<E: de::Error>(self, value: u128) -> Result<String, E> {
        Ok(value.to_string())
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<String, E> {
        Ok(value.to_string())
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<String, E> {
        Ok(value.to_string())
    }

    fn visit_char<E: de::Error>(self, value: char) -> Result<String, E> {
        Ok(value.to_string())
    }
}

/// A text setting.
pub(super) fn text<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    Text::deserialize(deserializer).map(|text| text.0)
}

/// An optional text setting.
pub(super) fn optional_text<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::<Text>::deserialize(deserializer).map(|text| text.map(|text| text.0))
}

/// A map of text settings.
pub(super) fn text_map<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    BTreeMap::<String, Text>::deserialize(deserializer)
        .map(|map| map.into_iter().map(|(key, text)| (key, text.0)).collect())
}
