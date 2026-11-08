//! Generates the Rust types, client and server of the query API.
//!
//! The schema is compiled with `protox`, a protobuf compiler written in
//! Rust, so building Burgess does not require `protoc` to be installed.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=proto");
    let descriptors = protox::compile(["burgess/query/v1/query.proto"], ["proto"])?;
    tonic_prost_build::configure()
        // Each client makes its own channel, thus this crate does not need
        // the tonic transport.
        .build_transport(false)
        // The JSON rows use a BTreeMap for meta. The same type here keeps
        // the conversion simple.
        .btree_map(".")
        .compile_fds(descriptors)?;
    Ok(())
}
