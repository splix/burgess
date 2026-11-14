//! Objects the writer stores next to the data files, and readers rely on.
//!
//! The query service finds data without listing the bucket: a per-day
//! manifest names every data file, with enough bounds to skip most of them
//! unopened, and the bloom filters of the day tell which days and files can
//! hold a reference or a tree. The writer and the query service are separate
//! processes, so the object formats live here where both see the same
//! definition.

mod bloom;
mod day;
mod file_layout;
mod frame;
mod manifest;
mod object_name;
mod ref_row;
mod state_row;
mod writer_id;

pub use bloom::{
    BloomError, BloomKey, BloomSize, DayBloom, Fpp, FppError, MAX_BLOOM_BYTES, RefsBloom, RefsKey,
    TreeKey, TreesBloom,
};
pub use day::{Day, DayError};
pub use file_layout::{RefsColumn, SCHEMA_VERSION, SCHEMA_VERSION_KEY, StatesColumn};
pub use frame::FrameError;
pub use manifest::{DayManifest, EntryProblem, FileStats, FlushEntry, ManifestError, StatesBounds};
pub use object_name::ObjectName;
pub use ref_row::RefRow;
pub use state_row::{EntryKind, StateEntry, StateRow};
pub use writer_id::{WriterId, WriterIdError};

/// Types generated from `proto/burgess/storage/v1/manifest.proto` and
/// `bloom.proto`.
///
/// Private so that other crates reach these objects only through
/// [`DayManifest`] and [`DayBloom`], which check what protobuf cannot, such
/// as tree id lengths and the format version.
pub(crate) mod proto {
    include!(concat!(env!("OUT_DIR"), "/burgess.storage.v1.rs"));
}
