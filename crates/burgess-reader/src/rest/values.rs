//! The values of options as REST takes them, read into the values of a
//! gRPC request.

use burgess_api::grpc::Order;
use burgess_api::parse_time;
use std::fmt;

use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer};

use super::error::RequestError;
use crate::pages::{LimitError, PageLimits};
use crate::queries::{self, TimeField};

/// A time, as text in a query string or a JSON string, or as a JSON number
/// of unix milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TimeValue {
    Millis(i64),
    Text(String),
}

/// A number of rows, as text in a query string, or as a JSON number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LimitValue {
    Number(u64),
    Text(String),
}

/// Each value is read by its own visitor, thus a value of a wrong type is
/// refused with what the option takes.
impl<'de> Deserialize<'de> for TimeValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Time;

        impl Visitor<'_> for Time {
            type Value = TimeValue;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("unix milliseconds or an RFC 3339 time")
            }

            fn visit_i64<E: de::Error>(self, ms: i64) -> Result<TimeValue, E> {
                Ok(TimeValue::Millis(ms))
            }

            fn visit_u64<E: de::Error>(self, ms: u64) -> Result<TimeValue, E> {
                i64::try_from(ms)
                    .map(TimeValue::Millis)
                    .map_err(|_| E::invalid_value(Unexpected::Unsigned(ms), &self))
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<TimeValue, E> {
                Ok(TimeValue::Text(text.to_string()))
            }
        }

        deserializer.deserialize_any(Time)
    }
}

impl<'de> Deserialize<'de> for LimitValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Limit;

        impl Visitor<'_> for Limit {
            type Value = LimitValue;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a number of rows")
            }

            fn visit_u64<E: de::Error>(self, rows: u64) -> Result<LimitValue, E> {
                Ok(LimitValue::Number(rows))
            }

            fn visit_i64<E: de::Error>(self, rows: i64) -> Result<LimitValue, E> {
                u64::try_from(rows)
                    .map(LimitValue::Number)
                    .map_err(|_| E::invalid_value(Unexpected::Signed(rows), &self))
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<LimitValue, E> {
                Ok(LimitValue::Text(text.to_string()))
            }
        }

        deserializer.deserialize_any(Limit)
    }
}

/// The time of the option `field` in unix milliseconds. REST also accepts
/// RFC 3339, as people write times.
pub(super) fn time_ms(
    field: TimeField,
    value: Option<&TimeValue>,
) -> Result<Option<i64>, RequestError> {
    match value {
        None => Ok(None),
        Some(TimeValue::Millis(ms)) => Ok(Some(*ms)),
        Some(TimeValue::Text(text)) => parse_time(text)
            .map(|time| Some(time.as_millisecond()))
            .map_err(|source| RequestError::Time { field, source }),
    }
}

/// The limit as gRPC sends it. A number too large for gRPC is over the
/// maximum, and REST refuses it with the same words.
pub(super) fn limit(
    value: Option<&LimitValue>,
    limits: &PageLimits,
) -> Result<Option<u32>, RequestError> {
    let requested = match value {
        None => return Ok(None),
        Some(LimitValue::Number(number)) => usize::try_from(*number).unwrap_or(usize::MAX),
        Some(LimitValue::Text(text)) => {
            text.parse().map_err(|_| RequestError::LimitNotANumber {
                found: text.clone(),
            })?
        }
    };
    let over_max = || {
        queries::RequestError::Limit(LimitError::OverMax {
            requested,
            max: limits.max_limit,
        })
    };
    Ok(Some(u32::try_from(requested).map_err(|_| over_max())?))
}

/// The order as gRPC sends it. REST names the two orders by time.
pub(super) fn order(text: Option<&str>) -> Result<i32, RequestError> {
    let order = match text {
        None => Order::Unspecified,
        Some("asc") => Order::TsAscending,
        Some("desc") => Order::TsDescending,
        Some(found) => {
            return Err(RequestError::Order {
                found: found.to_string(),
            });
        }
    };
    Ok(order as i32)
}
