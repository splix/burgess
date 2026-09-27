//! Why a message was left out of a batch.

use burgess_core::flow::FlowId;
use burgess_core::validation::ValidationError;
use burgess_core::wire::DecodeError;

/// Why a message was skipped instead of stored.
///
/// A skipped message is consumed like any other: the writer logs it and moves
/// on, because one bad producer must not stop every other producer's records
/// from being stored.
#[derive(Debug, thiserror::Error)]
pub(super) enum SkipReason {
    /// The message is not a readable record.
    #[error(transparent)]
    Decode(#[from] DecodeError),
    /// The record was read but breaks a validation rule.
    #[error("record of flow {flow_id} is invalid: {error}")]
    Invalid {
        /// The flow the record belongs to, to find the producer that sent it.
        flow_id: FlowId,
        /// The rule it broke.
        #[source]
        error: ValidationError,
    },
}

/// The rule a skipped message broke, without the details of the message.
///
/// Skips are counted by kind, so a batch full of the same mistake reports it
/// once with a count rather than once per message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum SkipKind {
    UnknownMarker,
    UnknownFramingVersion,
    UndecodableBody,
    FlowIdLength,
    MissingBody,
    EmptyReference,
    ReferenceCharacter,
    ReferenceTooLong,
    InvalidPayloadType,
    PayloadTooLarge,
}

impl SkipReason {
    pub(super) fn kind(&self) -> SkipKind {
        match self {
            SkipReason::Decode(error) => match error {
                DecodeError::UnknownMarker { .. } => SkipKind::UnknownMarker,
                DecodeError::UnknownFramingVersion { .. } => SkipKind::UnknownFramingVersion,
                DecodeError::UndecodableBody(_) => SkipKind::UndecodableBody,
                DecodeError::FlowIdLength { .. } => SkipKind::FlowIdLength,
                DecodeError::MissingBody => SkipKind::MissingBody,
            },
            SkipReason::Invalid { error, .. } => match error {
                ValidationError::Empty { .. } => SkipKind::EmptyReference,
                ValidationError::Character { .. } => SkipKind::ReferenceCharacter,
                ValidationError::TooLong { .. } => SkipKind::ReferenceTooLong,
                ValidationError::InvalidPayloadType { .. } => SkipKind::InvalidPayloadType,
                ValidationError::PayloadTooLarge { .. } => SkipKind::PayloadTooLarge,
            },
        }
    }
}
