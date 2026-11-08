//! The options of GET and POST, read in one way.
//!
//! GET and POST take the same options, thus a query string is read into
//! the JSON object of the same options, and the object of either is read
//! into one type. The two can then not drift apart, and a mistake gets the
//! same answer by either method.

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use super::error::RequestError;

/// The prefix of an option that names a label, as in `meta.region=eu`.
const META: &str = "meta.";

/// The options of `pairs`, read as a JSON object of strings into a `T`.
///
/// Each `meta.<key>` goes into the object `meta`. An option given two
/// times is refused, since the API would take only one of them, and the
/// caller could not tell which.
pub(super) fn read_pairs<T: DeserializeOwned>(
    pairs: Vec<(String, String)>,
) -> Result<T, RequestError> {
    let mut options = Map::new();
    let mut meta = Map::new();
    for (name, value) in pairs {
        let (map, key) = match name.strip_prefix(META) {
            Some(key) => (&mut meta, key.to_string()),
            None => (&mut options, name.clone()),
        };
        if map.insert(key, Value::String(value)).is_some() {
            return Err(RequestError::GivenTwice { name });
        }
    }
    if !meta.is_empty()
        && options
            .insert("meta".to_string(), Value::Object(meta))
            .is_some()
    {
        return Err(RequestError::GivenTwice {
            name: "meta".to_string(),
        });
    }
    read_options(Value::Object(options))
}

/// The options of `object`, the query string or the body, read into a `T`.
///
/// Only an object is read: serde would read an array into the fields in
/// their order, which no caller means.
pub(super) fn read_options<T: DeserializeOwned>(object: Value) -> Result<T, RequestError> {
    if !object.is_object() {
        return Err(RequestError::Options(
            "expected a JSON object of options".to_string(),
        ));
    }
    serde_json::from_value(object).map_err(|error| RequestError::Options(error.to_string()))
}
