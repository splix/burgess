//! What the writer and the query service agree on inside the Parquet files.
//!
//! The Arrow schema itself stays with the writer, which is the only one
//! encoding it. Readers need just the names to find columns and the version
//! to know they understand the file, and those must not drift apart.

/// Key of the file metadata entry holding the layout version.
pub const SCHEMA_VERSION_KEY: &str = "burgess.schema_version";

/// Version of the file layout, bumped when a reader of older files would
/// misread newer ones.
pub const SCHEMA_VERSION: &str = "1";

/// A column of the states file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatesColumn {
    /// The tree id as 12 raw bytes; the first sort key.
    Tree,
    /// The subflow; the second sort key.
    Subflow,
    /// Producer time; the third sort key.
    Ts,
    /// Whether the row is a state or a split.
    Kind,
    /// Service that wrote the row.
    Service,
    /// Category of the row.
    Category,
    /// The child of a split; empty on states.
    ChildSubflow,
    /// Labels of a state; empty on splits.
    Meta,
    /// How to read the payload; empty on splits.
    PayloadType,
    /// The captured state; empty on splits.
    Payload,
}

impl StatesColumn {
    /// The column name in the file.
    pub fn name(self) -> &'static str {
        match self {
            StatesColumn::Tree => "tree",
            StatesColumn::Subflow => "subflow",
            StatesColumn::Ts => "ts",
            StatesColumn::Kind => "kind",
            StatesColumn::Service => "service",
            StatesColumn::Category => "category",
            StatesColumn::ChildSubflow => "child_subflow",
            StatesColumn::Meta => "meta",
            StatesColumn::PayloadType => "payload_type",
            StatesColumn::Payload => "payload",
        }
    }
}
