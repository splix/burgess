//! Objects the writer stores next to the data files, and readers rely on.
//!
//! The query service finds data without listing the bucket: a per-day
//! manifest names every data file, with enough bounds to skip most of them
//! unopened. The writer and the query service are separate processes, so the
//! object formats live here where both see the same definition.

mod day;
mod frame;
mod manifest;
mod object_name;
mod writer_id;

pub use day::{Day, DayError};
pub use frame::FrameError;
pub use manifest::{DayManifest, EntryProblem, FileStats, FlushEntry, ManifestError, StatesBounds};
pub use object_name::ObjectName;
pub use writer_id::{WriterId, WriterIdError};

/// Types generated from `proto/burgess/storage/v1/manifest.proto`.
///
/// Private so that other crates reach manifests only through
/// [`DayManifest`], which checks what protobuf cannot, such as tree id
/// lengths and the format version.
pub(crate) mod proto {
    include!(concat!(env!("OUT_DIR"), "/burgess.storage.v1.rs"));
}
