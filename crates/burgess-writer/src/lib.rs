//! The single process that moves records from Kafka to object storage.
//!
//! Reads are rare and storage is cheap, so the writer does the work up front.
//! It batches and sorts the records, then writes Parquet files together with
//! small index objects. The index objects let the query service find a file
//! without listing the bucket.

pub mod source;

pub mod batch;

pub mod parquet_files;

pub mod flush;

pub mod config;

pub mod pipeline;

pub mod app;

#[cfg(test)]
mod test_support;

/// An error with all its sources, `outer: inner: innermost`, for log lines:
/// the outermost message alone rarely says what actually went wrong.
pub fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut chain = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        source = cause.source();
    }
    chain
}
