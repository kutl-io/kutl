/// The schema files, for both the rerun triggers and the compile call.
const PROTOS: &[&str] = &[
    "../../proto/kutl/sync/v1/sync.proto",
    "../../proto/kutl/daemon/v1/daemon.proto",
];

fn main() {
    // Point prost-build at the vendored protoc binary so the build is
    // self-contained — release runners (cargo-dist on macOS/Windows)
    // don't have protoc on PATH.
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("vendored protoc unavailable for this target");
    // SAFETY: single-threaded build script; no other thread reads env.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    // Regenerate when the schema changes — prost-build does not reliably emit
    // this, so an edited .proto would otherwise serve stale generated code.
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }

    let mut config = prost_build::Config::new();
    // The wire types serialize as JSON (HTTP surfaces, fixtures); the
    // daemon's on-disk messages are protobuf only.
    config.type_attribute(
        ".kutl.sync.v1",
        "#[derive(serde::Serialize, serde::Deserialize)]",
    );
    config
        .compile_protos(PROTOS, &["../../proto"])
        .expect("failed to compile kutl protos");
}
