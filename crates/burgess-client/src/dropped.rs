//! A record the emitter refused to send, described for the log.

use std::fmt;

use burgess_core::flow::FlowId;
use burgess_core::validation::ValidationError;

use crate::throttle::Verdict;

/// Why a record was not sent.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DropReason {
    /// The state payload could not be written as JSON.
    #[error("payload is not JSON: {0}")]
    NotJson(#[from] serde_json::Error),
    /// The writer would skip the record.
    #[error("the writer would skip it: {0}")]
    Invalid(#[from] ValidationError),
}

/// The kind of record, so a log line says which call made it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordKind {
    State,
    Split,
    Reference,
}

impl fmt::Display for RecordKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RecordKind::State => "state",
            RecordKind::Split => "split",
            RecordKind::Reference => "reference",
        })
    }
}

/// A dropped record, with what a developer needs to find the call that made
/// it. The flow id alone is random per execution and points at no code.
#[derive(Debug)]
pub(crate) struct DroppedRecord<'a> {
    pub service: &'a str,
    pub category: Option<&'a str>,
    pub kind: RecordKind,
    pub flow: FlowId,
}

impl DroppedRecord<'_> {
    /// Logs the drop, as a warning or quietly as the throttle decided.
    pub fn log(&self, reason: &DropReason, verdict: Verdict) {
        let DroppedRecord {
            service,
            category,
            kind,
            flow,
        } = self;
        match verdict {
            Verdict::Warn { suppressed } => tracing::warn!(
                service,
                category,
                %kind,
                %flow,
                suppressed,
                "dropping a record: {reason}"
            ),
            Verdict::Quiet => tracing::debug!(
                service,
                category,
                %kind,
                %flow,
                "dropping a record: {reason}"
            ),
        }
    }
}
