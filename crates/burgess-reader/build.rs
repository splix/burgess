//! Generates the Rust types of the cursor schema.
//!
//! The schema is compiled with `protox`, a protobuf compiler written in
//! Rust, so building Burgess does not require `protoc` to be installed.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=proto");
    let descriptors = protox::compile(["burgess/cursor/v1/cursor.proto"], ["proto"])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}
