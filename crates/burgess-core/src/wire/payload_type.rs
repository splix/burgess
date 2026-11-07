//! How payload types are spelled outside the wire format.

use super::proto::PayloadType;

impl PayloadType {
    /// Every payload type, so that names can be read back.
    const ALL: [PayloadType; 2] = [PayloadType::Unspecified, PayloadType::Json];

    /// The name stored in files and shown by the query API.
    ///
    /// Short and lowercase, unlike the protobuf constant names, because it is
    /// what investigators read. Listing every variant makes adding a payload
    /// type a compile error here until it has a name.
    pub fn name(self) -> &'static str {
        match self {
            PayloadType::Unspecified => "unspecified",
            PayloadType::Json => "json",
        }
    }

    /// The payload type stored as `name`, or `None` if no type has that name.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|payload_type| payload_type.name() == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_is_stored_as_json() {
        assert_eq!(PayloadType::Json.name(), "json");
    }

    #[test]
    fn every_protobuf_value_is_listed() {
        // Protobuf enums are numbered from zero without large gaps, so this
        // range covers any value the schema will plausibly get.
        for value in 0..1024 {
            if let Ok(payload_type) = PayloadType::try_from(value) {
                assert!(
                    PayloadType::ALL.contains(&payload_type),
                    "{payload_type:?} is missing from PayloadType::ALL"
                );
            }
        }
    }

    #[test]
    fn names_read_back() {
        for payload_type in PayloadType::ALL {
            assert_eq!(
                PayloadType::from_name(payload_type.name()),
                Some(payload_type)
            );
        }
    }

    #[test]
    fn unknown_name_is_none() {
        assert_eq!(PayloadType::from_name("PAYLOAD_TYPE_JSON"), None);
    }
}
