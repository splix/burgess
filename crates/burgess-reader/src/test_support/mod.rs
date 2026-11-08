//! Test doubles and fixtures shared by the reader's tests.

mod days;
mod recording_store;

pub(crate) use days::{Days, at, flush, minted_on, noon, put_day};
pub(crate) use recording_store::RecordingStore;

use std::collections::BTreeMap;

use burgess_core::flow::{Subflow, TreeId};
use burgess_core::storage::{Day, StateEntry, StateRow};
use burgess_core::wire::{PayloadType, UnixMillis};
use jiff::civil::Date;
use jiff::{Span, Timestamp};

/// The day `days` before today by the host clock, which the reader also
/// uses for retention, so test days stay within it whenever the tests run.
pub(crate) fn days_ago(days: i64) -> Day {
    let today = Date::from(Day::utc(Timestamp::now()).unwrap());
    Day::try_from(today.checked_sub(Span::new().days(days)).unwrap()).unwrap()
}

/// Runs `walk`, and gives the value of `day` from before and from after
/// it. A test that crosses midnight UTC sees two different days, and either
/// one is correct.
pub(crate) async fn around<T>(day: impl Fn() -> T, walk: impl Future<Output = ()>) -> [T; 2] {
    let before = day();
    walk.await;
    [before, day()]
}

/// A row of `subflow` in `tree` at `time` in unix milliseconds: a split to
/// `child`, or a state when there is none.
pub(crate) fn state_row(tree: TreeId, subflow: u32, time: u64, child: Option<u32>) -> StateRow {
    StateRow {
        tree,
        subflow: Subflow::from(subflow),
        time: UnixMillis::from(time),
        service: "checkout".into(),
        category: "step".into(),
        entry: match child {
            Some(child) => StateEntry::Split {
                child: Subflow::from(child),
            },
            None => StateEntry::State {
                meta: BTreeMap::new(),
                payload_type: PayloadType::Json,
                payload: b"{}".to_vec(),
            },
        },
    }
}

/// A state of `subflow` in `tree` at `time` in unix milliseconds, with
/// `payload` as stored.
pub(crate) fn state_with_payload(
    tree: TreeId,
    subflow: u32,
    time: u64,
    payload: &[u8],
) -> StateRow {
    let mut row = state_row(tree, subflow, time, None);
    if let StateEntry::State {
        payload: stored, ..
    } = &mut row.entry
    {
        *stored = payload.to_vec();
    }
    row
}

/// The subflow of each row, in order.
pub(crate) fn subflows(rows: &[StateRow]) -> Vec<u32> {
    rows.iter().map(|row| u32::from(row.subflow)).collect()
}

/// The messages of a gRPC answer, for a test that expects one.
pub(crate) async fn messages<R>(
    answer: Result<tonic::Response<crate::grpc::Answer<R>>, tonic::Status>,
) -> Vec<R> {
    use futures::StreamExt;

    answer
        .expect("an answer")
        .into_inner()
        .map(|message| message.expect("a message"))
        .collect()
        .await
}
