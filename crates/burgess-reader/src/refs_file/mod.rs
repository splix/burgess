//! Reading the references of one outside id out of one refs file.
//!
//! A refs file is sorted by `(ref_type, domain_id)`, thus the references of
//! one id are one range of rows. As for a states file, a read gets the tail
//! of the file first and selects only the pages whose bounds on the two key
//! columns include the key.

mod decode;
mod error;
mod key;
mod reader;

pub use error::{RefsFileProblem, RefsReadError};
pub use key::RefKey;
pub use reader::RefsFileReader;

#[cfg(test)]
mod tests;
