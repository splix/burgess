mod query_hash;
mod window;

use std::collections::BTreeMap;

use burgess_api::grpc::{ListStatesRequest, Order};
use burgess_core::storage::Day;
use burgess_core::wire::UnixMillis;
use jiff::Timestamp;
use jiff::civil::date;
use prost::Message;

use super::*;

fn at(ms: i64) -> Timestamp {
    Timestamp::from_millisecond(ms).unwrap()
}

fn window(since_ms: Option<i64>, until_ms: Option<i64>) -> Window {
    Window::new(since_ms.map(at), until_ms.map(at)).unwrap()
}

fn row() -> RowPosition {
    RowPosition {
        day: Day::try_from(date(2026, 11, 14)).unwrap(),
        nnnn: UnixMillis::from(1_794_650_000_123),
        index: 41,
    }
}

/// The time of the state in [`ordered`].
const TS: i64 = 1_794_649_999_000;

/// The oldest day of the walks in the tests, days before [`row`].
fn oldest() -> Day {
    Day::try_from(date(2026, 10, 15)).unwrap()
}

fn unordered(window: Window) -> Cursor {
    Cursor {
        window,
        position: Position::Unordered(row()),
        oldest: oldest(),
    }
}

fn ordered(window: Window) -> Cursor {
    Cursor {
        window,
        position: Position::Ordered {
            ts: UnixMillis::from(TS as u64),
            row: row(),
        },
        oldest: oldest(),
    }
}

fn last_day() -> Window {
    window(Some(1_794_563_600_000), Some(1_794_650_000_000))
}

fn states_request() -> ListStatesRequest {
    ListStatesRequest {
        service: "checkout".into(),
        category: "step".into(),
        r#where: Some("amount > 100".into()),
        meta: BTreeMap::from([
            ("region".into(), "eu".into()),
            ("env".into(), "prod".into()),
            ("tier".into(), "gold".into()),
        ]),
        since_ms: None,
        until_ms: None,
        limit: Some(50),
        order: Order::TsDescending.into(),
        cursor: None,
    }
}

/// The text of `cursor` for `given`, decoded for `next`.
fn continued<A: PagedRequest, B: PagedRequest>(
    cursor: Cursor,
    given: &A,
    next: &B,
) -> Result<Cursor, CursorError> {
    let text = cursor.encode(QueryHash::of(given));
    Cursor::decode(&text, QueryHash::of(next))
}

/// `message` as the text of a cursor, as a client could send it.
fn text_of(message: &proto::Cursor) -> String {
    TEXT.encode(&message.encode_to_vec())
}

fn valid_message(request: &impl PagedRequest) -> proto::Cursor {
    proto::Cursor {
        format_version: FORMAT_VERSION,
        query_hash: QueryHash::of(request).into(),
        since_ms: None,
        until_ms: Some(1_794_650_000_000),
        position: Some(Position::Unordered(row()).into()),
        oldest_day: oldest().to_string(),
    }
}

/// `message` decoded for `request`.
fn decoded(message: &proto::Cursor, request: &impl PagedRequest) -> Result<Cursor, CursorError> {
    Cursor::decode(&text_of(message), QueryHash::of(request))
}

#[test]
fn a_cursor_round_trips_with_each_position_and_window() {
    let request = states_request();
    let windows = [
        last_day(),
        window(None, Some(1_794_650_000_000)),
        window(Some(0), Some(1_794_650_000_000)),
        window(Some(1_794_563_600_000), None),
        window(None, None),
        window(Some(TS), Some(TS)),
    ];
    for window in windows {
        for cursor in [unordered(window), ordered(window)] {
            assert_eq!(continued(cursor, &request, &request).unwrap(), cursor);
        }
    }
}

#[test]
fn a_cursor_is_letters_and_digits_only() {
    let text = ordered(last_day()).encode(QueryHash::of(&states_request()));
    assert!(
        text.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "{text}"
    );
}

#[test]
fn a_cursor_continues_the_same_request_with_another_cursor_or_limit() {
    let first = states_request();
    let next = ListStatesRequest {
        limit: Some(10_000),
        cursor: Some("ANY".into()),
        ..first.clone()
    };
    let without_limit = ListStatesRequest {
        limit: None,
        ..first.clone()
    };
    let cursor = ordered(last_day());

    assert_eq!(continued(cursor, &first, &next).unwrap(), cursor);
    assert_eq!(continued(cursor, &first, &without_limit).unwrap(), cursor);
}

#[test]
fn a_cursor_is_refused_with_another_request() {
    let first = states_request();
    let other = ListStatesRequest {
        category: "done".into(),
        ..first.clone()
    };
    assert!(matches!(
        continued(ordered(last_day()), &first, &other),
        Err(CursorError::OtherQuery)
    ));
}

/// A cursor that one reader gives must continue on each other reader, also
/// of another build. Thus its text must not change with the build.
#[test]
fn a_cursor_has_the_same_text_on_each_build() {
    const TEXT: &str = "BAARCIB4DMW7SOO7JQMIBXONUKOTIIEASXT4XHJUGIOARGEN47FZ2NASCMFAQMRQGI3DCMJRGQIPXFPHZOOTIGBJHIEDEMBSGYYTAMJV";

    let cursor = ordered(last_day());
    let request = states_request();

    assert_eq!(cursor.encode(QueryHash::of(&request)), TEXT);
    assert_eq!(
        Cursor::decode(TEXT, QueryHash::of(&request)).unwrap(),
        cursor
    );
}

#[test]
fn an_empty_cursor_is_refused_as_empty() {
    assert!(matches!(
        Cursor::decode("", QueryHash::of(&states_request())),
        Err(CursorError::Empty)
    ));
}

#[test]
fn text_that_is_not_base32_is_refused() {
    let query = QueryHash::of(&states_request());
    for text in ["not a cursor", "abc!"] {
        let decoded = Cursor::decode(text, query);
        assert!(
            matches!(decoded, Err(CursorError::Text(_))),
            "{text:?}: {decoded:?}"
        );
    }
}

#[test]
fn bytes_that_are_not_a_cursor_are_refused() {
    let text = TEXT.encode(&[0xff; 7]);
    assert!(matches!(
        Cursor::decode(&text, QueryHash::of(&states_request())),
        Err(CursorError::Message(_))
    ));
}

#[test]
fn a_cursor_of_another_version_is_refused() {
    let request = states_request();
    for version in [0, FORMAT_VERSION + 1] {
        let message = proto::Cursor {
            format_version: version,
            ..valid_message(&request)
        };
        assert!(matches!(
            decoded(&message, &request),
            Err(CursorError::Version { found }) if found == version
        ));
    }
}

#[test]
fn a_cursor_without_a_position_is_refused() {
    let request = states_request();
    let no_position = proto::Cursor {
        position: None,
        ..valid_message(&request)
    };
    let no_row = proto::Cursor {
        position: Some(proto::cursor::Position::Ordered(proto::OrderedPosition {
            ts_ms: TS as u64,
            row: None,
        })),
        ..valid_message(&request)
    };
    for message in [no_position, no_row] {
        assert!(matches!(
            decoded(&message, &request),
            Err(CursorError::NoPosition)
        ));
    }
}

#[test]
fn a_cursor_with_an_invalid_day_is_refused() {
    let request = states_request();
    let message = proto::Cursor {
        position: Some(proto::cursor::Position::Unordered(proto::RowPosition {
            day: "2026-11-14".into(),
            ..row().into()
        })),
        ..valid_message(&request)
    };
    assert!(matches!(
        decoded(&message, &request),
        Err(CursorError::Day(_))
    ));
}

#[test]
fn a_cursor_with_a_time_that_cannot_bound_a_lookup_is_refused() {
    let request = states_request();
    let before_first_day = Timestamp::MIN.as_millisecond();
    for (since_ms, until_ms) in [
        (None, Some(i64::MAX)),
        (Some(i64::MIN), None),
        (Some(before_first_day), Some(1_794_650_000_000)),
    ] {
        let message = proto::Cursor {
            since_ms,
            until_ms,
            ..valid_message(&request)
        };
        let decoded = decoded(&message, &request);
        assert!(
            matches!(decoded, Err(CursorError::Window(WindowError::Time(_)))),
            "{since_ms:?}..{until_ms:?}: {decoded:?}"
        );
    }
}

#[test]
fn a_cursor_whose_window_ends_before_it_starts_is_refused() {
    let request = states_request();
    let message = proto::Cursor {
        since_ms: Some(1_794_650_000_001),
        until_ms: Some(1_794_650_000_000),
        ..valid_message(&request)
    };
    assert!(matches!(
        decoded(&message, &request),
        Err(CursorError::Window(WindowError::Reversed { .. }))
    ));
}

/// The rows of an answer are in its window, thus a position out of the
/// window is from no page.
#[test]
fn an_ordered_position_out_of_its_window_is_refused() {
    let request = states_request();
    let ordered_at = |ts_ms: u64| proto::Cursor {
        since_ms: Some(TS),
        until_ms: Some(TS + 1_000),
        position: Some(proto::cursor::Position::Ordered(proto::OrderedPosition {
            ts_ms,
            row: Some(row().into()),
        })),
        ..valid_message(&request)
    };
    let ts = TS as u64;
    for ts_ms in [ts - 1, ts + 1_001, u64::MAX] {
        assert!(
            matches!(
                decoded(&ordered_at(ts_ms), &request),
                Err(CursorError::OutOfWindow)
            ),
            "{ts_ms}"
        );
    }
    for ts_ms in [ts, ts + 1_000] {
        assert!(decoded(&ordered_at(ts_ms), &request).is_ok(), "{ts_ms}");
    }
}

/// Without its oldest day, the next page cannot know when retention
/// deleted a part of the answer.
#[test]
fn a_cursor_without_its_oldest_day_is_refused() {
    let request = states_request();
    let message = proto::Cursor {
        oldest_day: String::new(),
        ..valid_message(&request)
    };
    assert!(matches!(
        decoded(&message, &request),
        Err(CursorError::Day(_))
    ));
}
