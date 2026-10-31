//! What a caller's thread hands over to the worker.

use std::sync::mpsc::SyncSender;

use burgess_core::flow::{FlowId, Subflow};
use burgess_core::wire::{Body, PayloadType, Reference, Split, State, UnixMillis};
use serde::Serialize;

use crate::dropped::RecordKind;

/// A state payload not yet written as JSON.
///
/// Writing it is left to the worker, so even a large state costs the caller
/// only the move.
pub(crate) trait JsonPayload: Send {
    fn to_json(&self) -> serde_json::Result<Vec<u8>>;
}

impl<T: Serialize + Send> JsonPayload for T {
    fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }
}

/// A record as the caller left it: what only the caller can decide, the
/// flow and the time, with everything else left raw for the worker.
pub(crate) struct Pending {
    pub flow: FlowId,
    pub time: UnixMillis,
    pub body: PendingBody,
}

/// The body of a [`Pending`] record.
pub(crate) enum PendingBody {
    Split {
        category: &'static str,
        child: Subflow,
    },
    State {
        category: &'static str,
        meta: Vec<(String, String)>,
        payload: Box<dyn JsonPayload>,
    },
    Reference {
        ref_type: &'static str,
        domain_id: String,
    },
}

impl PendingBody {
    pub fn kind(&self) -> RecordKind {
        match self {
            PendingBody::Split { .. } => RecordKind::Split,
            PendingBody::State { .. } => RecordKind::State,
            PendingBody::Reference { .. } => RecordKind::Reference,
        }
    }

    pub fn category(&self) -> Option<&'static str> {
        match self {
            PendingBody::Split { category, .. } | PendingBody::State { category, .. } => {
                Some(category)
            }
            PendingBody::Reference { .. } => None,
        }
    }

    /// The wire body, with the payload written as JSON.
    pub fn into_body(self, service: &str) -> serde_json::Result<Body> {
        Ok(match self {
            PendingBody::Split { category, child } => Body::Split(Split {
                service: service.into(),
                category: category.into(),
                child_subflow: child.into(),
            }),
            PendingBody::State {
                category,
                meta,
                payload,
            } => Body::State(State {
                service: service.into(),
                category: category.into(),
                meta: meta.into_iter().collect(),
                payload_type: PayloadType::Json.into(),
                payload: payload.to_json()?,
            }),
            PendingBody::Reference {
                ref_type,
                domain_id,
            } => Body::Reference(Reference {
                ref_type: ref_type.into(),
                domain_id,
                service: service.into(),
                category: None,
            }),
        })
    }
}

/// One entry in the queue from the callers' threads to the worker.
pub(crate) enum Item {
    Record(Pending),
    /// Marks a point in the queue: the worker signals `done` once every
    /// record queued before it has reached the sink.
    Flush {
        done: SyncSender<()>,
    },
}
