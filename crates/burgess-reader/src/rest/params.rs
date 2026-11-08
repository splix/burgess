//! The options a lookup request takes from its query string.

use std::num::NonZeroUsize;

use burgess_api::parse_time;
use jiff::Timestamp;
use serde::Deserialize;

use super::error::RequestError;
use crate::pages::PageLimits;

/// The query string as given. Values are read as text and checked here, so
/// that a bad one is reported in the API's own error body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PageParams {
    until: Option<String>,
    limit: Option<String>,
}

/// The options of a lookup request, checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PageRequest {
    /// The last day the lookup must cover, as the lookup takes it.
    pub(super) until: Option<Timestamp>,
    /// The rows the page may hold.
    pub(super) limit: NonZeroUsize,
}

impl PageParams {
    pub(super) fn request(self, limits: &PageLimits) -> Result<PageRequest, RequestError> {
        let until = self.until.as_deref().map(parse_time).transpose()?;
        let limit = self
            .limit
            .map(|limit| {
                limit
                    .parse()
                    .map_err(|_| RequestError::LimitNotANumber { found: limit })
            })
            .transpose()?;
        Ok(PageRequest {
            until,
            limit: limits.limit(limit)?,
        })
    }
}
