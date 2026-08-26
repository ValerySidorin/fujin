fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../proto/grpc/v1/fujin.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["../../proto"])?;
    Ok(())
}
