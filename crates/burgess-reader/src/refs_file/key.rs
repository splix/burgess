//! The outside id that a refs lookup searches for.

use burgess_core::validation::{RefField, ValidationError};

/// The kind and the value of an outside id, as the writer stores them.
///
/// The writer trims both values and refuses values that break the rules of
/// a reference. The key gets the same treatment, thus it matches what was
/// stored, and a value that is never stored is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefKey {
    ref_type: String,
    domain_id: String,
}

impl RefKey {
    /// The key of `ref_type` and `domain_id`, normalized.
    pub fn new(mut ref_type: String, mut domain_id: String) -> Result<Self, ValidationError> {
        RefField::RefType.normalize(&mut ref_type)?;
        RefField::DomainId.normalize(&mut domain_id)?;
        Ok(RefKey {
            ref_type,
            domain_id,
        })
    }

    /// The kind of id, for example `order_id`.
    pub fn ref_type(&self) -> &str {
        &self.ref_type
    }

    /// The id.
    pub fn domain_id(&self) -> &str {
        &self.domain_id
    }
}
