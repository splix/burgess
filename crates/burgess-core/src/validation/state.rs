//! Rules for state records.

use super::error::ValidationError;
use super::{Limits, Validate};
use crate::wire::{PayloadType, State};

/// Service, category and meta have no rules yet and are stored as sent.
impl Validate for State {
    fn validate(&mut self, limits: &Limits) -> Result<(), ValidationError> {
        // Listing every known type, rather than rejecting only `Unspecified`,
        // makes adding a type to the schema a compile error here until someone
        // decides whether it can be stored and served.
        match PayloadType::try_from(self.payload_type) {
            Ok(PayloadType::Json) => {}
            Ok(PayloadType::Unspecified) | Err(_) => {
                return Err(ValidationError::InvalidPayloadType {
                    found: self.payload_type,
                });
            }
        }
        if self.payload.len() > limits.max_payload_bytes {
            return Err(ValidationError::PayloadTooLarge {
                found: self.payload.len(),
                max: limits.max_payload_bytes,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const SMALL: Limits = Limits {
        max_payload_bytes: 10,
    };

    fn state(payload_bytes: usize) -> State {
        State {
            service: "checkout".into(),
            payload_type: PayloadType::Json.into(),
            payload: vec![b'x'; payload_bytes],
            ..State::default()
        }
    }

    #[test]
    fn payload_at_limit_passes() {
        assert_eq!(state(10).validate(&SMALL), Ok(()));
    }

    #[test]
    fn payload_over_limit_fails() {
        assert_eq!(
            state(11).validate(&SMALL),
            Err(ValidationError::PayloadTooLarge { found: 11, max: 10 })
        );
    }

    #[test]
    fn limit_counts_payload_alone() {
        let mut state = State {
            meta: BTreeMap::from([("key".into(), "v".repeat(100))]),
            ..state(10)
        };
        assert_eq!(state.validate(&SMALL), Ok(()));
    }

    #[test]
    fn default_limit_is_one_mebibyte() {
        let limits = Limits::default();
        assert_eq!(state(1024 * 1024).validate(&limits), Ok(()));
        assert!(state(1024 * 1024 + 1).validate(&limits).is_err());
    }

    #[test]
    fn rejects_unset_and_unknown_payload_types() {
        for found in [PayloadType::Unspecified.into(), 99] {
            let mut state = State {
                payload_type: found,
                ..state(1)
            };
            assert_eq!(
                state.validate(&SMALL),
                Err(ValidationError::InvalidPayloadType { found })
            );
        }
    }
}
