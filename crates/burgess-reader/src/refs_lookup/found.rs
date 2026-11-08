//! What a refs lookup found.

use burgess_core::storage::RefRow;

use super::error::RefsLookupError;

/// The references a lookup could read, and what it could not read.
///
/// A lookup continues past a day or a file that it cannot read: an
/// investigation wants all references that can still be found, and the
/// problems tell which can be missing.
#[derive(Debug, Default)]
pub struct FoundRefs {
    /// The references found, newest day first, newest flush first in a day,
    /// and in the order of the file in a flush.
    pub rows: Vec<RefRow>,
    /// Each manifest or file that could not be read, in walk order.
    pub problems: Vec<RefsLookupError>,
}
