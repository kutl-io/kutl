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

    let mut config = prost_build::Config::new();
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    config
        .compile_protos(&["../../proto/kutl/sync/v1/sync.proto"], &["../../proto"])
        .unwrap();
}
