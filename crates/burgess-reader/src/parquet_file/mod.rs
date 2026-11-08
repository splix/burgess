//! Reading the Parquet files of the writer from the object store.
//!
//! The states reader and the refs reader do the same steps: get the tail of
//! the file by the size in the manifest, check the layout, and select only
//! the pages whose bounds on the sort key include the key. This module has
//! those steps one time, in `open_selected`. The readers only decode the
//! batches.

mod checks;
mod config;
mod open;
mod problem;
mod selection;
mod stored_file;

pub(crate) use checks::leaf_column;
pub use config::FileReadConfig;
pub(crate) use open::open_selected;
pub use problem::FileProblem;
pub(crate) use selection::ColumnKey;
