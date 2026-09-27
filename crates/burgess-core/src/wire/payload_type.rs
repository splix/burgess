//! How payload types are spelled outside the wire format.

use super::proto::PayloadType;

impl PayloadType {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_is_stored_as_json() {
        assert_eq!(PayloadType::Json.name(), "json");
    }
}
