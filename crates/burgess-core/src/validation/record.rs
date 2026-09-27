//! Validation of a whole record, which is validation of its body.

use super::error::ValidationError;
use super::{Limits, Validate};
use crate::wire::{Body, Record, Split};

/// The flow id was checked while decoding and the time has no rules yet, so a
/// record is as valid as its body.
impl Validate for Record {
    fn validate(&mut self, limits: &Limits) -> Result<(), ValidationError> {
        self.body.validate(limits)
    }
}

impl Validate for Body {
    fn validate(&mut self, limits: &Limits) -> Result<(), ValidationError> {
        match self {
            Body::State(state) => state.validate(limits),
            Body::Split(split) => split.validate(limits),
            Body::Reference(reference) => reference.validate(limits),
        }
    }
}

/// No rules yet: even a zero child subflow is stored as sent, until the spec
/// decides how to treat it.
impl Validate for Split {
    fn validate(&mut self, _limits: &Limits) -> Result<(), ValidationError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Reference, State, UnixMillis};

    fn record(body: Body) -> Record {
        Record {
            flow_id: "0192f3a1b2c3d4e5f6a7b8c900000000".parse().unwrap(),
            time: UnixMillis::from(1_790_000_000_123),
            body,
        }
    }

    #[test]
    fn trims_the_reference_in_place() {
        let mut record = record(Body::Reference(Reference {
            ref_type: " order_id ".into(),
            domain_id: " ORD-1 ".into(),
            service: "orders".into(),
            category: None,
        }));
        record.validate(&Limits::default()).unwrap();
        let Body::Reference(reference) = record.body else {
            panic!("expected a reference");
        };
        assert_eq!(
            (reference.ref_type.as_str(), reference.domain_id.as_str()),
            ("order_id", "ORD-1")
        );
    }

    #[test]
    fn rejected_record_stays_available_for_the_log() {
        let mut record = record(Body::State(State::default()));
        assert!(record.validate(&Limits::default()).is_err());
        assert_eq!(
            record.flow_id.to_string(),
            "0192f3a1b2c3d4e5f6a7b8c900000000"
        );
    }

    #[test]
    fn keeps_what_the_spec_has_not_decided() {
        let split = record(Body::Split(Split {
            service: String::new(),
            category: String::new(),
            child_subflow: 0,
        }));
        let mut validated = split.clone();
        assert_eq!(validated.validate(&Limits::default()), Ok(()));
        assert_eq!(validated, split);
    }
}
