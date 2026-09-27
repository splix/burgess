//! Request and response types of the query API.
//!
//! The query service produces these types and the CLI consumes them. With one
//! shared definition, renaming a field on the server breaks the CLI build
//! instead of silently breaking its output.
