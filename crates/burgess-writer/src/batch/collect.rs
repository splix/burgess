//! A batch being filled, and the sorted batch it becomes.

use std::collections::BTreeMap;

use burgess_core::validation::{Limits, Validate};
use burgess_core::wire::Record;

use super::rows::{RefRow, Row, StateRow};
use super::skip::{SkipKind, SkipReason};
use crate::source::{LogEntry, PartitionId, Position};

/// A finished batch, sorted and ready to be written.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SortedBatch {
    /// States and splits, sorted by tree, subflow and time.
    pub states: Vec<StateRow>,
    /// References, sorted by reference type and outside id.
    pub refs: Vec<RefRow>,
    /// Where to resume after this batch, for each partition that delivered
    /// entries to it, skipped ones included. Partitions that delivered nothing
    /// are absent, so these positions update the previous ones rather than
    /// replace them.
    pub positions: BTreeMap<PartitionId, Position>,
    /// How many messages were skipped as malformed.
    pub skipped: usize,
}

/// A batch still receiving entries.
#[derive(Debug, Default)]
pub(super) struct Batch {
    states: Vec<StateRow>,
    refs: Vec<RefRow>,
    positions: BTreeMap<PartitionId, Position>,
    skips: BTreeMap<SkipKind, usize>,
    bytes: usize,
}

impl Batch {
    /// Adds the record in `entry`, or skips it if it is malformed.
    ///
    /// Only the first skip of each kind in a batch is logged in full, so one
    /// misbehaving producer cannot flood the log; the rest are counted and
    /// reported when the batch is sorted.
    pub(super) fn add(&mut self, entry: LogEntry, limits: &Limits) {
        self.positions.insert(entry.partition, entry.resume_at);
        match valid_record(&entry.payload, limits) {
            Ok(record) => {
                self.bytes += entry.payload.len();
                match Row::from(record) {
                    Row::State(row) => self.states.push(row),
                    Row::Ref(row) => self.refs.push(row),
                }
            }
            Err(reason) => {
                let count = self.skips.entry(reason.kind()).or_default();
                *count += 1;
                if *count == 1 {
                    tracing::error!(
                        partition = %entry.partition,
                        resume_at = %entry.resume_at,
                        %reason,
                        "skipping malformed record"
                    );
                }
            }
        }
    }

    /// Bytes of the messages kept so far.
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Sorts the rows for writing, and logs how many messages were skipped.
    ///
    /// The sort is stable, so rows with equal keys keep the order they were
    /// read in, which within a partition is the order they were produced.
    pub(super) fn into_sorted(mut self) -> SortedBatch {
        self.states.sort_by_key(StateRow::sort_key);
        self.refs.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        let skipped = self.skips.values().sum();
        if skipped > 0 {
            tracing::error!(skipped, by_reason = ?self.skips, "skipped malformed records in batch");
        }
        SortedBatch {
            states: self.states,
            refs: self.refs,
            positions: self.positions,
            skipped,
        }
    }
}

fn valid_record(message: &[u8], limits: &Limits) -> Result<Record, SkipReason> {
    let mut record = Record::decode(message)?;
    record
        .validate(limits)
        .map_err(|error| SkipReason::Invalid {
            flow_id: record.flow_id,
            error,
        })?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use burgess_core::flow::{FlowId, Subflow};
    use burgess_core::wire::{Body, PayloadType, Reference, Split, State, UnixMillis};

    use super::*;
    use crate::batch::StateEntry;

    const ROOT: &str = "0192f3a1b2c3d4e5f6a7b8c900000000";
    const CHILD: &str = "0192f3a1b2c3d4e5f6a7b8c900000007";
    const OTHER_TREE: &str = "0192f3a1b2c3ffffffffffff00000000";

    fn encode(flow: &str, ms: u64, body: Body) -> Vec<u8> {
        Record {
            flow_id: flow.parse().unwrap(),
            time: UnixMillis::from(ms),
            body,
        }
        .encode()
    }

    fn state(flow: &str, ms: u64, service: &str) -> Vec<u8> {
        encode(
            flow,
            ms,
            Body::State(State {
                service: service.into(),
                payload_type: PayloadType::Json.into(),
                payload: b"{}".to_vec(),
                ..State::default()
            }),
        )
    }

    fn reference(flow: &str, ref_type: &str, domain_id: &str) -> Vec<u8> {
        encode(
            flow,
            1,
            Body::Reference(Reference {
                ref_type: ref_type.into(),
                domain_id: domain_id.into(),
                service: "orders".into(),
                category: None,
            }),
        )
    }

    fn entry(partition: u32, resume_at: u64, payload: Vec<u8>) -> LogEntry {
        LogEntry {
            partition: PartitionId::from(partition),
            payload,
            resume_at: Position::from(resume_at),
        }
    }

    fn batch_of(payloads: Vec<Vec<u8>>) -> SortedBatch {
        let mut batch = Batch::default();
        for (i, payload) in payloads.into_iter().enumerate() {
            batch.add(entry(0, i as u64 + 1, payload), &Limits::default());
        }
        batch.into_sorted()
    }

    fn services(batch: &SortedBatch) -> Vec<&str> {
        batch
            .states
            .iter()
            .map(|row| row.service.as_str())
            .collect()
    }

    #[test]
    fn malformed_message_in_the_middle_is_skipped() {
        let batch = batch_of(vec![
            state(ROOT, 1, "first"),
            b"not a record".to_vec(),
            state(ROOT, 2, "last"),
        ]);
        assert_eq!(services(&batch), ["first", "last"]);
        assert_eq!(batch.skipped, 1);
    }

    #[test]
    fn invalid_record_in_the_middle_is_skipped() {
        let batch = batch_of(vec![
            reference(ROOT, "order_id", "ORD-1"),
            reference(ROOT, "order id", "ORD-2"),
            reference(ROOT, "order_id", "ORD-3"),
        ]);
        let ids: Vec<&str> = batch
            .refs
            .iter()
            .map(|row| row.domain_id.as_str())
            .collect();
        assert_eq!(ids, ["ORD-1", "ORD-3"]);
        assert_eq!(batch.skipped, 1);
    }

    #[test]
    fn positions_cover_skipped_messages() {
        let mut batch = Batch::default();
        batch.add(entry(0, 5, state(ROOT, 1, "kept")), &Limits::default());
        batch.add(entry(0, 6, b"bad".to_vec()), &Limits::default());
        batch.add(entry(1, 3, state(ROOT, 1, "kept")), &Limits::default());
        let positions = batch.into_sorted().positions;
        assert_eq!(
            positions,
            BTreeMap::from([
                (PartitionId::from(0), Position::from(6)),
                (PartitionId::from(1), Position::from(3)),
            ])
        );
    }

    #[test]
    fn split_is_filed_under_its_parent() {
        let batch = batch_of(vec![encode(
            ROOT,
            1,
            Body::Split(Split {
                service: "checkout".into(),
                category: "fan-out".into(),
                child_subflow: 7,
            }),
        )]);
        let parent: FlowId = ROOT.parse().unwrap();
        let row = &batch.states[0];
        assert_eq!((row.tree, row.subflow), (parent.tree(), parent.subflow()));
        assert_eq!(
            row.entry,
            StateEntry::Split {
                child: Subflow::from(7)
            }
        );
    }

    #[test]
    fn states_are_sorted_by_tree_subflow_and_time() {
        let batch = batch_of(vec![
            state(OTHER_TREE, 1, "other tree"),
            state(CHILD, 1, "child"),
            state(ROOT, 20, "root late"),
            state(ROOT, 10, "root early"),
        ]);
        assert_eq!(
            services(&batch),
            ["root early", "root late", "child", "other tree"]
        );
    }

    #[test]
    fn equal_keys_keep_arrival_order() {
        let batch = batch_of(vec![
            state(ROOT, 1, "first"),
            state(ROOT, 1, "second"),
            state(ROOT, 1, "third"),
        ]);
        assert_eq!(services(&batch), ["first", "second", "third"]);
    }

    #[test]
    fn refs_are_sorted_by_type_and_id() {
        let batch = batch_of(vec![
            reference(ROOT, "user_id", "U-1"),
            reference(ROOT, "order_id", "ORD-2"),
            reference(ROOT, "order_id", "ORD-1"),
        ]);
        let keys: Vec<(&str, &str)> = batch.refs.iter().map(RefRow::sort_key).collect();
        assert_eq!(
            keys,
            [
                ("order_id", "ORD-1"),
                ("order_id", "ORD-2"),
                ("user_id", "U-1")
            ]
        );
    }

    #[test]
    fn stored_reference_is_trimmed() {
        let batch = batch_of(vec![reference(ROOT, " order_id ", " ORD-1 ")]);
        assert_eq!(batch.refs[0].sort_key(), ("order_id", "ORD-1"));
    }

    #[test]
    fn counts_bytes_of_kept_messages_only() {
        let kept = state(ROOT, 1, "kept");
        let kept_len = kept.len();
        let mut batch = Batch::default();
        batch.add(entry(0, 1, kept), &Limits::default());
        batch.add(entry(0, 2, vec![0; 100]), &Limits::default());
        assert_eq!(batch.bytes(), kept_len);
    }
}
