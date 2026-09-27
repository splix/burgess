//! The day that groups a batch's files.

use std::fmt;
use std::str::FromStr;

use jiff::Timestamp;
use jiff::civil::Date;
use jiff::tz::Offset;

/// The text form of a day.
const FORMAT: &str = "%Y%m%d";

/// A calendar day by the writer clock, written `YYYYMMDD`.
///
/// Days name the object prefixes and each manifest records its own, so the
/// text form must be exact: eight digits, no separators. Only years with four
/// digits have that form, so days outside them are refused up front rather
/// than written in a form that could not be read back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Day(Date);

/// Why a day could not be made or read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DayError {
    /// The text is not a day in `YYYYMMDD` form.
    #[error("{found:?} is not a day in YYYYMMDD form")]
    NotYyyymmdd {
        /// The rejected text.
        found: String,
    },
    /// The year does not have four digits.
    #[error("year {year} cannot be written with four digits")]
    YearOutOfRange {
        /// The rejected year.
        year: i16,
    },
}

impl Day {
    /// The UTC day of `time`.
    ///
    /// UTC, so the day boundary does not move with the writer host's time
    /// zone or daylight saving, and matches the UTC timestamps in the files.
    pub fn utc(time: Timestamp) -> Result<Self, DayError> {
        Day::try_from(Offset::UTC.to_datetime(time).date())
    }
}

impl TryFrom<Date> for Day {
    type Error = DayError;

    fn try_from(date: Date) -> Result<Self, Self::Error> {
        if !(0..=9999).contains(&date.year()) {
            return Err(DayError::YearOutOfRange { year: date.year() });
        }
        Ok(Day(date))
    }
}

impl From<Day> for Date {
    fn from(day: Day) -> Self {
        day.0
    }
}

impl fmt::Display for Day {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.strftime(FORMAT))
    }
}

impl FromStr for Day {
    type Err = DayError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || DayError::NotYyyymmdd {
            found: text.to_string(),
        };
        // The parser alone would also take signs and shorter fields.
        if text.len() != 8 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        Date::strptime(FORMAT, text).map(Day).map_err(|_| invalid())
    }
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;

    use super::*;

    fn day(year: i16, month: i8, day: i8) -> Day {
        Day::try_from(date(year, month, day)).unwrap()
    }

    #[test]
    fn text_round_trip() {
        assert_eq!(day(2026, 9, 7).to_string(), "20260907");
        assert_eq!("20260907".parse::<Day>(), Ok(day(2026, 9, 7)));
    }

    #[test]
    fn years_at_both_ends_round_trip() {
        for day in [day(0, 1, 1), day(9999, 12, 31)] {
            assert_eq!(day.to_string().parse::<Day>(), Ok(day), "{day}");
        }
    }

    /// Dates end at year 9999, so only negative years can fall outside.
    #[test]
    fn refuses_negative_years() {
        assert_eq!(
            Day::try_from(date(-1, 1, 1)),
            Err(DayError::YearOutOfRange { year: -1 })
        );
    }

    #[test]
    fn rejects_other_forms() {
        for text in [
            "2026-09-07",
            "2026097",
            "202609070",
            "20261301",
            "20260230",
            "2026O907",
            "+2026090",
            "",
        ] {
            assert_eq!(
                text.parse::<Day>(),
                Err(DayError::NotYyyymmdd {
                    found: text.to_string()
                }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn utc_day_changes_at_utc_midnight() {
        let before: Timestamp = "2026-09-27T23:59:59.999Z".parse().unwrap();
        let after: Timestamp = "2026-09-28T00:00:00Z".parse().unwrap();
        assert_eq!(Day::utc(before), Ok(day(2026, 9, 27)));
        assert_eq!(Day::utc(after), Ok(day(2026, 9, 28)));
    }

    #[test]
    fn orders_by_date() {
        assert!(day(2026, 9, 7) < day(2026, 10, 1));
    }
}
