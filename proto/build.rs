use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from("./src/generated/");

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .compile_protos(&["proto/core.proto"], &["proto/"])?;

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .compile_protos(&["proto/analytics.proto"], &["proto/"])?;

    Ok(())
}
