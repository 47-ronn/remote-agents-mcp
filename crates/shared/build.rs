//! Generate Rust types from the protobuf wire schema (`proto/remote_agents.proto`).
//!
//! Uses `protox` (a pure-Rust `.proto` compiler) so no system `protoc` binary is
//! required — important for CI and the `cross` musl build container. The compiled
//! `FileDescriptorSet` is handed to `prost-build`, which writes the generated
//! module to `OUT_DIR` (included via `pub mod proto` in lib.rs).

use std::path::PathBuf;

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    // proto/ lives at the workspace root, two levels above crates/shared.
    let proto_dir = manifest.join("../../proto");
    let proto_file = proto_dir.join("remote_agents.proto");

    let fds = protox::compile([&proto_file], [&proto_dir])
        .expect("failed to compile proto/remote_agents.proto with protox");

    prost_build::Config::new()
        .compile_fds(fds)
        .expect("prost-build code generation failed");

    println!("cargo:rerun-if-changed={}", proto_file.display());
}
