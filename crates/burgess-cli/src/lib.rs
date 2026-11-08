//! The command line client of the query service.
//!
//! It talks only to the query service, over gRPC. Thus an investigator
//! needs only the address of the service: no Kafka access and no object
//! store credentials.
//!
//! Ids and times are checked here before any request is sent, so a mistyped
//! id is reported at once, in the same words the service would use.

pub mod args;
pub mod client;
pub mod output;
pub mod requests;
pub mod run;
pub mod server;

#[cfg(test)]
mod test_support;
