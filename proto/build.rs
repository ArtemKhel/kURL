use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from("./src/generated/");
    let protoc_bin = protoc_bin_vendored::protoc_bin_path()?;

    unsafe { std::env::set_var("PROTOC", protoc_bin) };

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .type_attribute(".core", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["proto/core.proto"], &["proto/"])?;

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .compile_protos(&["proto/analytics.proto"], &["proto/"])?;

    Ok(())
}
