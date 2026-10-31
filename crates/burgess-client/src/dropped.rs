//! A record the emitter refused to send, described for the log.

use std::fmt;

use burgess_core::flow::FlowId;
use burgess_core::validation::ValidationError;
use burgess_core::wire::{Body, Record};

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

impl From<&Body> for RecordKind {
    fn from(body: &Body) -> Self {
        match body {
            Body::State(_) => RecordKind::State,
            Body::Split(_) => RecordKind::Split,
            Body::Reference(_) => RecordKind::Reference,
        }
    }
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
    pub reason: DropReason,
}

impl<'a> DroppedRecord<'a> {
    /// Describes a record that was built but must not be sent.
    pub fn of(service: &'a str, record: &'a Record, reason: DropReason) -> Self {
        let category = match &record.body {
            Body::State(state) => Some(state.category.as_str()),
            Body::Split(split) => Some(split.category.as_str()),
            Body::Reference(reference) => reference.category.as_deref(),
        };
        DroppedRecord {
            service,
            category,
            kind: RecordKind::from(&record.body),
            flow: record.flow_id,
            reason,
        }
    }

    /// Logs the drop, as a warning or quietly as the throttle decided.
    pub fn log(&self, verdict: Verdict) {
        let DroppedRecord {
            service,
            category,
            kind,
            flow,
            reason,
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
