//! What a refs lookup searches for.

use burgess_core::storage::RefRow;
use jiff::Timestamp;

use crate::refs_file::RefKey;

/// The references to look up: an outside id, and filters on the rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefsQuery {
    /// The outside id.
    pub key: RefKey,
    /// Only references that this service recorded.
    pub service: Option<String>,
    /// Only references recorded in this category.
    pub category: Option<String>,
    /// Only references at this time or later, by the producer clock.
    pub since: Option<Timestamp>,
    /// Only references at this time or earlier, by the producer clock.
    pub until: Option<Timestamp>,
}

impl RefsQuery {
    /// Whether `row`, a reference to the key, passes the filters.
    pub(super) fn admits(&self, row: &RefRow) -> bool {
        let ts = row.time.saturating_timestamp();
        self.service
            .as_ref()
            .is_none_or(|service| *service == row.service)
            && self
                .category
                .as_ref()
                .is_none_or(|category| row.category.as_ref() == Some(category))
            && self.since.is_none_or(|since| since <= ts)
            && self.until.is_none_or(|until| ts <= until)
    }
}
