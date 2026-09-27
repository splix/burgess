//! Generates the Rust types of the wire and storage schemas.
//!
//! The schemas are compiled with `protox`, a protobuf compiler written in
//! Rust, so building Burgess does not require `protoc` to be installed.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=proto");
    let descriptors = protox::compile(
        [
            "burgess/wire/v1/record.proto",
            "burgess/storage/v1/manifest.proto",
        ],
        ["proto"],
    )?;
    prost_build::Config::new()
        // A sorted map encodes the same record to the same bytes every time,
        // which keeps encoded messages comparable in tests and across runs.
        .btree_map(["."])
        .compile_fds(descriptors)?;
    Ok(())
}
