//! The day manifest: which data files exist for a day.
//!
//! It is the only authority on visibility. The writer lists a flush here only
//! after its data files are stored, so readers never see half a flush, and
//! files it does not list are ignored even if they exist.

use crate::flow::TreeId;
use crate::wire::UnixMillis;

use super::day::{Day, DayError};
use super::frame::{self, FrameError};
use super::proto;

/// Starts every manifest object.
const MAGIC: [u8; 4] = *b"BGSM";

/// Version of the manifest's meaning this build writes and reads.
const FORMAT_VERSION: u32 = 1;

/// Why bytes are not a manifest this build can use.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    /// The object is not a well-formed `BGSM` frame.
    #[error(transparent)]
    Frame(#[from] FrameError),
    /// Bytes follow the manifest, which never has any.
    #[error("{count} unexpected bytes after the manifest")]
    TrailingBytes {
        /// How many.
        count: usize,
    },
    /// Written by a newer writer whose manifests this build cannot interpret.
    #[error("manifest format version {found} is not supported, expected {FORMAT_VERSION}")]
    UnsupportedVersion {
        /// The version in the manifest.
        found: u32,
    },
    /// The day is not in `YYYYMMDD` form.
    #[error(transparent)]
    Day(#[from] DayError),
    /// A tree bound is not a 12-byte tree id.
    #[error("tree bound has {found} bytes, expected 12")]
    TreeLength {
        /// Its length.
        found: usize,
    },
    /// An entry contradicts itself, so readers would skip or misplace rows.
    #[error("flush entry {seq} is invalid: {problem}")]
    InvalidEntry {
        /// The entry's batch counter.
        seq: u64,
        /// What is wrong with it.
        problem: EntryProblem,
    },
}

/// How a flush entry contradicts itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EntryProblem {
    /// The states file has rows but the entry has no bounds, so every lookup
    /// by tree or time would skip rows that exist.
    #[error("the states file has rows but no bounds")]
    MissingBounds,
    /// The entry has bounds but the states file has no rows to bound.
    #[error("the states file has bounds but no rows")]
    BoundsWithoutRows,
    /// The smallest tree is larger than the largest.
    #[error("min_tree is larger than max_tree")]
    InvertedTrees,
    /// The earliest time is later than the latest.
    #[error("min_ts is later than max_ts")]
    InvertedTimes,
}

/// The data files of one day, as the writer last recorded them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayManifest {
    /// The day the files belong to.
    pub day: Day,
    /// The writer that owns the day.
    pub writer_id: String,
    /// Whether the day is final. A sealed manifest never changes again, so
    /// readers may cache it indefinitely.
    pub sealed: bool,
    /// One entry per flush, in flush order.
    pub entries: Vec<FlushEntry>,
}

/// One flush: a states file and a refs file written together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushEntry {
    /// Flush time by the writer clock. It also names the flush's files.
    pub nnnn: UnixMillis,
    /// The writer's batch counter, one more at every flush.
    pub seq: u64,
    /// Bounds of the states file's rows, or `None` when it has no rows: a
    /// lookup by tree or time then skips it without opening it.
    pub states_bounds: Option<StatesBounds>,
    /// The states file.
    pub states: FileStats,
    /// The refs file.
    pub refs: FileStats,
}

/// Smallest and largest values in a states file.
///
/// The tree bounds are exact, so a tree outside them is certainly not in the
/// file. The time bounds cover states rows only, which is what time-ranged
/// state listings prune on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatesBounds {
    /// Smallest tree id in the file.
    pub min_tree: TreeId,
    /// Largest tree id in the file.
    pub max_tree: TreeId,
    /// Earliest producer time in the file.
    pub min_ts: UnixMillis,
    /// Latest producer time in the file.
    pub max_ts: UnixMillis,
}

/// Size of one data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FileStats {
    /// Rows in the file.
    pub rows: u64,
    /// Size of the file in bytes.
    pub bytes: u64,
}

impl DayManifest {
    /// An empty, unsealed manifest, for the first flush of a day.
    pub fn new(day: Day, writer_id: String) -> Self {
        DayManifest {
            day,
            writer_id,
            sealed: false,
            entries: Vec::new(),
        }
    }

    /// The manifest as a framed object, ready to store.
    ///
    /// Refuses an inconsistent entry, which a reader would refuse too: better
    /// to fail the flush than to store a manifest nobody can read.
    pub fn encode(&self) -> Result<Vec<u8>, ManifestError> {
        self.check_entries()?;
        Ok(frame::encode(MAGIC, &proto::DayManifest::from(self), &[]))
    }

    /// Reads a stored manifest.
    pub fn decode(bytes: &[u8]) -> Result<Self, ManifestError> {
        let (manifest, trailing) = frame::decode::<proto::DayManifest>(MAGIC, bytes)?;
        if !trailing.is_empty() {
            return Err(ManifestError::TrailingBytes {
                count: trailing.len(),
            });
        }
        let manifest = DayManifest::try_from(manifest)?;
        manifest.check_entries()?;
        Ok(manifest)
    }

    fn check_entries(&self) -> Result<(), ManifestError> {
        self.entries.iter().try_for_each(|entry| {
            entry
                .check()
                .map_err(|problem| ManifestError::InvalidEntry {
                    seq: entry.seq,
                    problem,
                })
        })
    }
}

impl FlushEntry {
    /// Checks that the bounds agree with the states file they describe.
    fn check(&self) -> Result<(), EntryProblem> {
        match (&self.states_bounds, self.states.rows) {
            (None, 0) => Ok(()),
            (None, _) => Err(EntryProblem::MissingBounds),
            (Some(_), 0) => Err(EntryProblem::BoundsWithoutRows),
            (Some(bounds), _) if bounds.min_tree > bounds.max_tree => {
                Err(EntryProblem::InvertedTrees)
            }
            (Some(bounds), _) if bounds.min_ts > bounds.max_ts => Err(EntryProblem::InvertedTimes),
            (Some(_), _) => Ok(()),
        }
    }
}

impl From<&DayManifest> for proto::DayManifest {
    fn from(manifest: &DayManifest) -> Self {
        proto::DayManifest {
            format_version: FORMAT_VERSION,
            day: manifest.day.to_string(),
            writer_id: manifest.writer_id.clone(),
            sealed: manifest.sealed,
            entries: manifest
                .entries
                .iter()
                .map(proto::FlushEntry::from)
                .collect(),
        }
    }
}

impl From<&FlushEntry> for proto::FlushEntry {
    fn from(entry: &FlushEntry) -> Self {
        proto::FlushEntry {
            nnnn: entry.nnnn.into(),
            seq: entry.seq,
            states_bounds: entry.states_bounds.map(|bounds| proto::StatesBounds {
                min_tree: bounds.min_tree.as_bytes().to_vec(),
                max_tree: bounds.max_tree.as_bytes().to_vec(),
                min_ts: bounds.min_ts.into(),
                max_ts: bounds.max_ts.into(),
            }),
            states_rows: entry.states.rows,
            refs_rows: entry.refs.rows,
            states_bytes: entry.states.bytes,
            refs_bytes: entry.refs.bytes,
        }
    }
}

impl TryFrom<proto::DayManifest> for DayManifest {
    type Error = ManifestError;

    fn try_from(manifest: proto::DayManifest) -> Result<Self, Self::Error> {
        if manifest.format_version != FORMAT_VERSION {
            return Err(ManifestError::UnsupportedVersion {
                found: manifest.format_version,
            });
        }
        Ok(DayManifest {
            day: manifest.day.parse()?,
            writer_id: manifest.writer_id,
            sealed: manifest.sealed,
            entries: manifest
                .entries
                .into_iter()
                .map(FlushEntry::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<proto::FlushEntry> for FlushEntry {
    type Error = ManifestError;

    fn try_from(entry: proto::FlushEntry) -> Result<Self, Self::Error> {
        Ok(FlushEntry {
            nnnn: UnixMillis::from(entry.nnnn),
            seq: entry.seq,
            states_bounds: entry
                .states_bounds
                .map(StatesBounds::try_from)
                .transpose()?,
            states: FileStats {
                rows: entry.states_rows,
                bytes: entry.states_bytes,
            },
            refs: FileStats {
                rows: entry.refs_rows,
                bytes: entry.refs_bytes,
            },
        })
    }
}

impl TryFrom<proto::StatesBounds> for StatesBounds {
    type Error = ManifestError;

    fn try_from(bounds: proto::StatesBounds) -> Result<Self, Self::Error> {
        Ok(StatesBounds {
            min_tree: tree(&bounds.min_tree)?,
            max_tree: tree(&bounds.max_tree)?,
            min_ts: UnixMillis::from(bounds.min_ts),
            max_ts: UnixMillis::from(bounds.max_ts),
        })
    }
}

fn tree(bytes: &[u8]) -> Result<TreeId, ManifestError> {
    <[u8; 12]>::try_from(bytes)
        .map(TreeId::from)
        .map_err(|_| ManifestError::TreeLength { found: bytes.len() })
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;

    use super::*;

    fn entry(nnnn: u64, seq: u64) -> FlushEntry {
        FlushEntry {
            nnnn: UnixMillis::from(nnnn),
            seq,
            states_bounds: Some(StatesBounds {
                min_tree: TreeId::from([1; 12]),
                max_tree: TreeId::from([9; 12]),
                min_ts: UnixMillis::from(nnnn - 5_000),
                max_ts: UnixMillis::from(nnnn - 10),
            }),
            states: FileStats {
                rows: 1_000,
                bytes: 65_536,
            },
            refs: FileStats {
                rows: 20,
                bytes: 2_048,
            },
        }
    }

    fn manifest() -> DayManifest {
        DayManifest {
            day: Day::try_from(date(2026, 9, 27)).unwrap(),
            writer_id: "writer-1".into(),
            sealed: false,
            entries: vec![entry(1_790_000_000_000, 41), entry(1_790_000_600_000, 42)],
        }
    }

    /// Frames a raw manifest message, to store what `DayManifest` would
    /// never write.
    fn framed(message: &proto::DayManifest) -> Vec<u8> {
        frame::encode(MAGIC, message, &[])
    }

    #[test]
    fn round_trip() {
        let manifest = manifest();
        assert_eq!(
            DayManifest::decode(&manifest.encode().unwrap()),
            Ok(manifest)
        );
    }

    #[test]
    fn starts_with_its_magic() {
        assert_eq!(manifest().encode().unwrap()[..4], *b"BGSM");
    }

    #[test]
    fn entry_without_states_has_no_bounds() {
        let mut manifest = manifest();
        manifest.entries[0].states_bounds = None;
        manifest.entries[0].states = FileStats::default();
        assert_eq!(
            DayManifest::decode(&manifest.encode().unwrap()),
            Ok(manifest)
        );
    }

    #[test]
    fn adding_an_entry_keeps_the_earlier_ones() {
        let stored = manifest().encode().unwrap();
        let mut manifest = DayManifest::decode(&stored).unwrap();
        manifest.entries.push(entry(1_790_001_200_000, 43));
        let reread = DayManifest::decode(&manifest.encode().unwrap()).unwrap();
        let seqs: Vec<u64> = reread.entries.iter().map(|entry| entry.seq).collect();
        assert_eq!(seqs, [41, 42, 43]);
        assert_eq!(reread.entries[..2], self::manifest().entries[..]);
    }

    #[test]
    fn seal_survives_a_round_trip() {
        let mut manifest = manifest();
        manifest.sealed = true;
        assert!(
            DayManifest::decode(&manifest.encode().unwrap())
                .unwrap()
                .sealed
        );
    }

    #[test]
    fn new_manifest_is_empty_and_open() {
        let manifest =
            DayManifest::new(Day::try_from(date(2026, 9, 27)).unwrap(), "writer-1".into());
        assert!(!manifest.sealed);
        assert!(manifest.entries.is_empty());
    }

    #[test]
    fn rejects_another_kind_of_object() {
        let mut bytes = manifest().encode().unwrap();
        bytes[3] = b'B';
        assert!(matches!(
            DayManifest::decode(&bytes),
            Err(ManifestError::Frame(FrameError::WrongMagic { .. }))
        ));
    }

    #[test]
    fn rejects_truncated_length() {
        let bytes = manifest().encode().unwrap();
        assert!(matches!(
            DayManifest::decode(&bytes[..5]),
            Err(ManifestError::Frame(FrameError::Truncated { .. }))
        ));
    }

    #[test]
    fn rejects_truncated_body() {
        let bytes = manifest().encode().unwrap();
        assert!(matches!(
            DayManifest::decode(&bytes[..bytes.len() - 3]),
            Err(ManifestError::Frame(FrameError::Truncated { .. }))
        ));
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bytes = manifest().encode().unwrap();
        bytes.extend_from_slice(b"xx");
        assert_eq!(
            DayManifest::decode(&bytes),
            Err(ManifestError::TrailingBytes { count: 2 })
        );
    }

    #[test]
    fn rejects_newer_format_version() {
        let mut message = proto::DayManifest::from(&manifest());
        message.format_version = 2;
        assert_eq!(
            DayManifest::decode(&framed(&message)),
            Err(ManifestError::UnsupportedVersion { found: 2 })
        );
    }

    #[test]
    fn rejects_malformed_day() {
        let mut message = proto::DayManifest::from(&manifest());
        message.day = "2026-09-27".into();
        assert!(matches!(
            DayManifest::decode(&framed(&message)),
            Err(ManifestError::Day(_))
        ));
    }

    #[test]
    fn rejects_tree_bound_of_wrong_length() {
        let mut message = proto::DayManifest::from(&manifest());
        message.entries[1]
            .states_bounds
            .as_mut()
            .unwrap()
            .max_tree
            .truncate(11);
        assert_eq!(
            DayManifest::decode(&framed(&message)),
            Err(ManifestError::TreeLength { found: 11 })
        );
    }

    /// Readers keep every manifest of the retention window in memory, so the
    /// size of an entry is part of the design; this pins it.
    #[test]
    fn realistic_entry_adds_72_bytes() {
        let nnnn = 1_790_000_000_000;
        let realistic = FlushEntry {
            nnnn: UnixMillis::from(nnnn),
            seq: 50_000,
            states_bounds: Some(StatesBounds {
                min_tree: TreeId::from([0x01; 12]),
                max_tree: TreeId::from([0xfe; 12]),
                min_ts: UnixMillis::from(nnnn - 600_000),
                max_ts: UnixMillis::from(nnnn),
            }),
            states: FileStats {
                rows: 7_000,
                bytes: 30 * 1024 * 1024,
            },
            refs: FileStats {
                rows: 1_000,
                bytes: 100 * 1024,
            },
        };
        let mut manifest =
            DayManifest::new(Day::try_from(date(2026, 9, 27)).unwrap(), "writer-1".into());
        let empty = manifest.encode().unwrap().len();
        manifest.entries.push(realistic);
        assert_eq!(manifest.encode().unwrap().len() - empty, 72);
    }

    #[test]
    fn refuses_entries_that_contradict_themselves() {
        type Breaks = fn(&mut FlushEntry);
        let cases: [(Breaks, EntryProblem); 4] = [
            (
                |entry| entry.states_bounds = None,
                EntryProblem::MissingBounds,
            ),
            (
                |entry| entry.states.rows = 0,
                EntryProblem::BoundsWithoutRows,
            ),
            (
                |entry| {
                    let bounds = entry.states_bounds.as_mut().unwrap();
                    std::mem::swap(&mut bounds.min_tree, &mut bounds.max_tree);
                },
                EntryProblem::InvertedTrees,
            ),
            (
                |entry| {
                    let bounds = entry.states_bounds.as_mut().unwrap();
                    std::mem::swap(&mut bounds.min_ts, &mut bounds.max_ts);
                },
                EntryProblem::InvertedTimes,
            ),
        ];
        for (break_entry, problem) in cases {
            let mut manifest = manifest();
            break_entry(&mut manifest.entries[1]);
            let expected = Some(ManifestError::InvalidEntry { seq: 42, problem });
            assert_eq!(manifest.encode().err(), expected, "encoding, {problem:?}");
            let framed = framed(&proto::DayManifest::from(&manifest));
            assert_eq!(
                DayManifest::decode(&framed).err(),
                expected,
                "decoding, {problem:?}"
            );
        }
    }

    #[test]
    fn single_row_file_has_equal_bounds() {
        let mut manifest = manifest();
        let bounds = manifest.entries[0].states_bounds.as_mut().unwrap();
        bounds.max_tree = bounds.min_tree;
        bounds.max_ts = bounds.min_ts;
        manifest.entries[0].states.rows = 1;
        assert!(manifest.encode().is_ok());
    }
}
