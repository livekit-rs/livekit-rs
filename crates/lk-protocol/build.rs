//! Generates Rust types from the pinned LiveKit protobuf schema (C-01).
//! Pure Rust toolchain: protox parses .proto files, no `protoc` binary is needed.
//!
//! Two type sets are generated from the same descriptors because upstream uses two
//! protojson dialects (impl-notes/api-auth-webhook.md §2.2, §4.3):
//! * `livekit` — lowerCamelCase, defaults omitted (webhooks, token roomConfig,
//!   default protojson). Used everywhere in code.
//! * `twirp_json::livekit` — original snake_case names, defaults emitted (twirp v8
//!   responses). Only used to render Twirp JSON responses; values are converted via
//!   the binary encoding, which is identical for both sets.
//!
//! Both accept either name style on input and ignore unknown fields.

use std::path::{Path, PathBuf};

const FILES: &[&str] = &[
    "livekit_models.proto",
    "livekit_internal.proto",
    "livekit_phone_number.proto",
    "rpc/io.proto",
    "rpc/agent.proto",
    "rpc/keepalive.proto",
    "rpc/signal.proto",
    "rpc/whip_signal.proto",
    "rpc/egress.proto",
    "rpc/ingress.proto",
    "rpc/sip.proto",
    "livekit_metrics.proto",
    "livekit_rtc.proto",
    "livekit_room.proto",
    "livekit_egress.proto",
    "livekit_ingress.proto",
    "livekit_sip.proto",
    "livekit_agent.proto",
    "livekit_agent_dispatch.proto",
    "livekit_webhook.proto",
    "logger/options.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("protos");
    let out = PathBuf::from(std::env::var("OUT_DIR")?);
    println!("cargo:rerun-if-changed={}", root.display());

    let files: Vec<PathBuf> = FILES.iter().map(|f| root.join(f)).collect();
    let options = root.join("psrpc");
    let mut compiler = protox::Compiler::new([&root, &options])?;
    compiler
        .include_source_info(true)
        .include_imports(true)
        .open_files(&files)?;
    // Real fixed protox GoogleFileResolver inputs, not hand-written schemas.
    compiler.open_file("google/protobuf/struct.proto")?;
    compiler.open_file("google/protobuf/field_mask.proto")?;
    let fds = compiler.file_descriptor_set();

    let mut descriptor_bytes = Vec::new();
    prost::Message::encode(&fds, &mut descriptor_bytes)?;

    std::fs::write(out.join("livekit-descriptors.bin"), &descriptor_bytes)?;
    generate(&fds, &descriptor_bytes, &out, false)?;
    let twirp_out = out.join("twirp_json");
    std::fs::create_dir_all(&twirp_out)?;
    generate(&fds, &descriptor_bytes, &twirp_out, true)?;
    // psrpc is deliberately compiled separately: package `internal` is not a
    // LiveKit public message and must not join the two public JSON dialects.
    let rpc_out = out.join("psrpc");
    std::fs::create_dir_all(&rpc_out)?;
    let rpc_fds = protox::compile([root.join("psrpc/internal.proto")], [&root])?;
    std::fs::write(
        out.join("psrpc-descriptors.bin"),
        prost::Message::encode_to_vec(&rpc_fds),
    )?;
    let mut rpc = prost_build::Config::new();
    rpc.out_dir(rpc_out);
    rpc.compile_well_known_types();
    rpc.extern_path(".google.protobuf", "::pbjson_types");
    rpc.disable_comments(["."]);
    rpc.compile_fds(rpc_fds)?;
    Ok(())
}

fn generate(
    fds: &prost_types::FileDescriptorSet,
    descriptor_bytes: &[u8],
    out: &Path,
    twirp_dialect: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = prost_build::Config::new();
    prost.out_dir(out);
    prost.enable_type_names();
    prost.compile_well_known_types();
    prost.extern_path(".google.protobuf", "::pbjson_types");
    prost.disable_comments(["."]);
    prost.compile_fds(fds.clone())?;

    let mut json = pbjson_build::Builder::new();
    json.register_descriptors(descriptor_bytes)?;
    json.out_dir(out)
        .extern_path(".google.protobuf", "::pbjson_types")
        .ignore_unknown_fields();
    if twirp_dialect {
        json.preserve_proto_field_names().emit_fields();
    }
    json.build(&[".livekit"])?;
    Ok(())
}
