//! How far the writer has stored, so a restart continues from there.
//!
//! Stored as `checkpoint/<writer_id>.json`. The location and format are
//! provisional: JSON keeps the checkpoint easy to inspect and fix by hand
//! while the design settles.

use std::collections::BTreeMap;

use burgess_core::storage::Day;
use burgess_core::wire::UnixMillis;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};

use crate::source::{PartitionId, Position};

/// Everything the writer needs to continue after a restart.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Checkpoint {
    /// Flushes so far; the next flush is `seq + 1`.
    pub seq: u64,
    /// Per partition, where to resume reading.
    pub positions: BTreeMap<PartitionId, Position>,
    /// Flush time of the last stored flush, `None` before the first one.
    pub last_flush_nnnn: Option<UnixMillis>,
    /// A flush whose manifest write may have happened without its final
    /// checkpoint. Recorded before the manifest write, so a restart knows
    /// exactly which day may list a flush this checkpoint does not cover.
    pub pending: Option<PendingFlush>,
    /// Days that have stored flushes but are not sealed yet, oldest first.
    pub unsealed: Vec<Day>,
}

/// A flush that had started writing its manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingFlush {
    /// The day whose manifest it writes.
    pub day: Day,
    /// Its flush time.
    pub nnnn: UnixMillis,
}

/// The JSON form: `{ seq, offsets: { <partition>: <offset> }, last_flush_nnnn,
/// pending: { day, nnnn } | null, unsealed: [<day>] }`.
#[derive(Serialize, Deserialize)]
struct Stored {
    seq: u64,
    offsets: BTreeMap<u32, u64>,
    last_flush_nnnn: Option<u64>,
    #[serde(default)]
    pending: Option<StoredPending>,
    #[serde(default)]
    unsealed: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct StoredPending {
    day: String,
    nnnn: u64,
}

impl Checkpoint {
    /// The checkpoint to store just before writing the manifest of a flush.
    pub(super) fn with_pending(&self, pending: PendingFlush) -> Checkpoint {
        Checkpoint {
            pending: Some(pending),
            ..self.clone()
        }
    }

    /// The checkpoint after a flush named `nnnn` that stored reads up to
    /// `positions`, with `unsealed` as the days still to seal.
    ///
    /// Positions are merged into the previous ones: a batch lists only the
    /// partitions that delivered entries, and a quiet partition must keep
    /// its place.
    pub(super) fn after_flush(
        &self,
        nnnn: UnixMillis,
        positions: &BTreeMap<PartitionId, Position>,
        unsealed: Vec<Day>,
    ) -> Checkpoint {
        let mut merged = self.positions.clone();
        merged.extend(positions);
        Checkpoint {
            seq: self.seq + 1,
            positions: merged,
            last_flush_nnnn: Some(nnnn),
            pending: None,
            unsealed,
        }
    }

    pub(super) fn to_json(&self) -> Vec<u8> {
        let stored = Stored {
            seq: self.seq,
            offsets: self
                .positions
                .iter()
                .map(|(partition, position)| (u32::from(*partition), u64::from(*position)))
                .collect(),
            last_flush_nnnn: self.last_flush_nnnn.map(u64::from),
            pending: self.pending.map(|pending| StoredPending {
                day: pending.day.to_string(),
                nnnn: pending.nnnn.into(),
            }),
            unsealed: self.unsealed.iter().map(Day::to_string).collect(),
        };
        serde_json::to_vec_pretty(&stored).expect("numbers and strings always serialize")
    }

    pub(super) fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let stored: Stored = serde_json::from_slice(bytes)?;
        let day = |text: &str| text.parse::<Day>().map_err(serde_json::Error::custom);
        let pending = match stored.pending {
            Some(pending) => Some(PendingFlush {
                day: day(&pending.day)?,
                nnnn: UnixMillis::from(pending.nnnn),
            }),
            None => None,
        };
        Ok(Checkpoint {
            seq: stored.seq,
            positions: stored
                .offsets
                .into_iter()
                .map(|(partition, offset)| (PartitionId::from(partition), Position::from(offset)))
                .collect(),
            last_flush_nnnn: stored.last_flush_nnnn.map(UnixMillis::from),
            pending,
            unsealed: stored
                .unsealed
                .iter()
                .map(|text| day(text))
                .collect::<Result<_, _>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn positions(pairs: &[(u32, u64)]) -> BTreeMap<PartitionId, Position> {
        pairs
            .iter()
            .map(|(partition, position)| (PartitionId::from(*partition), Position::from(*position)))
            .collect()
    }

    fn day(text: &str) -> Day {
        text.parse().unwrap()
    }

    #[test]
    fn json_round_trip() {
        let checkpoint = Checkpoint {
            seq: 7,
            positions: positions(&[(0, 120), (3, 9)]),
            last_flush_nnnn: Some(UnixMillis::from(1_790_000_000_000)),
            pending: Some(PendingFlush {
                day: day("20260927"),
                nnnn: UnixMillis::from(1_790_000_600_000),
            }),
            unsealed: vec![day("20260925"), day("20260926")],
        };
        assert_eq!(
            Checkpoint::from_json(&checkpoint.to_json()).unwrap(),
            checkpoint
        );
    }

    #[test]
    fn json_has_the_documented_shape() {
        let json = br#"{"seq": 2, "offsets": {"0": 10, "1": 20}, "last_flush_nnnn": 1790000000000,
            "pending": {"day": "20260927", "nnnn": 1790000000001}, "unsealed": ["20260926"]}"#;
        let checkpoint = Checkpoint::from_json(json).unwrap();
        assert_eq!(checkpoint.seq, 2);
        assert_eq!(checkpoint.positions, positions(&[(0, 10), (1, 20)]));
        assert_eq!(
            checkpoint.pending,
            Some(PendingFlush {
                day: day("20260927"),
                nnnn: UnixMillis::from(1_790_000_000_001)
            })
        );
        assert_eq!(checkpoint.unsealed, [day("20260926")]);
    }

    #[test]
    fn older_checkpoints_without_the_new_fields_still_read() {
        let json = br#"{"seq": 2, "offsets": {}, "last_flush_nnnn": null}"#;
        let checkpoint = Checkpoint::from_json(json).unwrap();
        assert_eq!(checkpoint.pending, None);
        assert!(checkpoint.unsealed.is_empty());
    }

    #[test]
    fn rejects_a_malformed_day() {
        let json =
            br#"{"seq": 2, "offsets": {}, "last_flush_nnnn": null, "unsealed": ["2026-09-26"]}"#;
        assert!(Checkpoint::from_json(json).is_err());
    }

    #[test]
    fn after_flush_counts_merges_positions_and_clears_pending() {
        let before = Checkpoint {
            seq: 4,
            positions: positions(&[(0, 100), (1, 50)]),
            last_flush_nnnn: Some(UnixMillis::from(1)),
            pending: Some(PendingFlush {
                day: day("20260927"),
                nnnn: UnixMillis::from(2),
            }),
            unsealed: vec![day("20260926")],
        };
        let after = before.after_flush(
            UnixMillis::from(2),
            &positions(&[(1, 60), (2, 5)]),
            Vec::new(),
        );
        assert_eq!(after.seq, 5);
        assert_eq!(after.positions, positions(&[(0, 100), (1, 60), (2, 5)]));
        assert_eq!(after.last_flush_nnnn, Some(UnixMillis::from(2)));
        assert_eq!(after.pending, None);
        assert!(after.unsealed.is_empty());
    }
}
