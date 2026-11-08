//! Ids in the JSON they travel in.
//!
//! Flow and tree ids are hex text, as investigators read and type them, and
//! a subflow is the plain number the id ends with.

use std::fmt::Display;
use std::str::FromStr;

use burgess_core::flow::Subflow;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serializer};

/// An id written as its text form.
pub(crate) mod text {
    use super::*;

    pub(crate) fn serialize<S: Serializer, T: Display>(
        id: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(id)
    }

    pub(crate) fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: FromStr<Err: Display>,
    {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(D::Error::custom)
    }
}

/// A subflow written as its number.
pub(crate) mod subflow {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(
        subflow: &Subflow,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(u32::from(*subflow))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Subflow, D::Error> {
        u32::deserialize(deserializer).map(Subflow::from)
    }
}

/// A subflow that may be absent, written as its number or `null`.
pub(crate) mod optional_subflow {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(
        subflow: &Option<Subflow>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match subflow {
            Some(subflow) => serializer.serialize_some(&u32::from(*subflow)),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Subflow>, D::Error> {
        Option::<u32>::deserialize(deserializer).map(|subflow| subflow.map(Subflow::from))
    }
}
