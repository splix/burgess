//! How far the writer has stored, so a restart continues from there.
//!
//! Stored as `_checkpoint/<writer_id>.json`. The location and format are
//! provisional: JSON keeps the checkpoint easy to inspect and fix by hand
//! while the design settles.

use std::collections::BTreeMap;

use burgess_core::wire::UnixMillis;
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
    /// Manifest entries newer than this were written by a flush that never
    /// reached its checkpoint.
    pub last_flush_nnnn: Option<UnixMillis>,
}

/// The JSON form, `{ seq, offsets: { <partition>: <offset> }, last_flush_nnnn }`.
#[derive(Serialize, Deserialize)]
struct Stored {
    seq: u64,
    offsets: BTreeMap<u32, u64>,
    last_flush_nnnn: Option<u64>,
}

impl Checkpoint {
    /// The checkpoint after a flush named `nnnn` that stored reads up to
    /// `positions`.
    ///
    /// Positions are merged into the previous ones: a batch lists only the
    /// partitions that delivered entries, and a quiet partition must keep
    /// its place.
    pub(super) fn after_flush(
        &self,
        nnnn: UnixMillis,
        positions: &BTreeMap<PartitionId, Position>,
    ) -> Checkpoint {
        let mut merged = self.positions.clone();
        merged.extend(positions);
        Checkpoint {
            seq: self.seq + 1,
            positions: merged,
            last_flush_nnnn: Some(nnnn),
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
        };
        serde_json::to_vec_pretty(&stored).expect("a map of numbers always serializes")
    }

    pub(super) fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let stored: Stored = serde_json::from_slice(bytes)?;
        Ok(Checkpoint {
            seq: stored.seq,
            positions: stored
                .offsets
                .into_iter()
                .map(|(partition, offset)| (PartitionId::from(partition), Position::from(offset)))
                .collect(),
            last_flush_nnnn: stored.last_flush_nnnn.map(UnixMillis::from),
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

    #[test]
    fn json_round_trip() {
        let checkpoint = Checkpoint {
            seq: 7,
            positions: positions(&[(0, 120), (3, 9)]),
            last_flush_nnnn: Some(UnixMillis::from(1_790_000_000_000)),
        };
        assert_eq!(
            Checkpoint::from_json(&checkpoint.to_json()).unwrap(),
            checkpoint
        );
    }

    #[test]
    fn json_has_the_documented_shape() {
        let json =
            br#"{"seq": 2, "offsets": {"0": 10, "1": 20}, "last_flush_nnnn": 1790000000000}"#;
        let checkpoint = Checkpoint::from_json(json).unwrap();
        assert_eq!(checkpoint.seq, 2);
        assert_eq!(checkpoint.positions, positions(&[(0, 10), (1, 20)]));
        assert_eq!(
            checkpoint.last_flush_nnnn,
            Some(UnixMillis::from(1_790_000_000_000))
        );
    }

    #[test]
    fn after_flush_counts_and_merges_positions() {
        let before = Checkpoint {
            seq: 4,
            positions: positions(&[(0, 100), (1, 50)]),
            last_flush_nnnn: Some(UnixMillis::from(1)),
        };
        let after = before.after_flush(UnixMillis::from(2), &positions(&[(1, 60), (2, 5)]));
        assert_eq!(after.seq, 5);
        assert_eq!(after.positions, positions(&[(0, 100), (1, 60), (2, 5)]));
        assert_eq!(after.last_flush_nnnn, Some(UnixMillis::from(2)));
    }
}
