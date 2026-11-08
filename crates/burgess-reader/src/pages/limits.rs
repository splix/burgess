//! How many rows one page may hold.

use std::num::NonZeroUsize;

/// How many rows a page holds when the request does not say, and the most
/// it may ask for.
///
/// The maximum bounds the rows a client gets in one response. It does not
/// bound what a request costs the service: a lookup reads the whole tree
/// before the page is cut, and rows count the same whatever their payload
/// size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLimits {
    /// Rows in a page whose request gives no limit.
    pub default_limit: NonZeroUsize,
    /// The largest limit a request may give.
    pub max_limit: NonZeroUsize,
}

/// Why a request's limit cannot be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LimitError {
    /// A page of no rows answers nothing.
    #[error("limit must be at least 1")]
    Zero,
    /// The limit is over the maximum. Refused rather than lowered, so the
    /// caller learns the maximum instead of mistaking a short page for all
    /// there is.
    #[error("limit {requested} is over the maximum of {max}")]
    OverMax {
        /// The limit asked for.
        requested: usize,
        /// The largest limit allowed.
        max: NonZeroUsize,
    },
}

/// Why the limits cannot be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the default limit {default} is over the maximum limit {max}")]
pub struct PageLimitsError {
    default: NonZeroUsize,
    max: NonZeroUsize,
}

impl PageLimits {
    /// Whether the limits fit together: a request without a limit must not
    /// be refused for asking too much.
    pub fn check(&self) -> Result<(), PageLimitsError> {
        if self.default_limit > self.max_limit {
            return Err(PageLimitsError {
                default: self.default_limit,
                max: self.max_limit,
            });
        }
        Ok(())
    }

    /// The rows a page holds for a request that asked for `requested`.
    pub fn limit(&self, requested: Option<usize>) -> Result<NonZeroUsize, LimitError> {
        let Some(requested) = requested else {
            return Ok(self.default_limit);
        };
        let limit = NonZeroUsize::new(requested).ok_or(LimitError::Zero)?;
        if limit > self.max_limit {
            return Err(LimitError::OverMax {
                requested,
                max: self.max_limit,
            });
        }
        Ok(limit)
    }
}

impl Default for PageLimits {
    /// 100 rows by default and at most 10,000; a deployment may change both.
    fn default() -> Self {
        PageLimits {
            default_limit: NonZeroUsize::new(100).expect("not zero"),
            max_limit: NonZeroUsize::new(10_000).expect("not zero"),
        }
    }
}
