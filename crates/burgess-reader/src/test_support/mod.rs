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
