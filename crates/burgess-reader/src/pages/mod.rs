//! What a lookup found, as the messages of a gRPC answer, and the limits of
//! a page. REST makes its page from the same messages.

mod cut;
mod limits;
mod page;
mod refs;
mod row;
mod states;

pub use limits::{LimitError, PageLimits, PageLimitsError};

#[cfg(test)]
mod tests;
