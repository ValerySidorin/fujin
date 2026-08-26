fn main() {
    println!("cargo:rerun-if-env-changed=VERSION");
    let version = std::env::var("VERSION")
        .unwrap_or_else(|_| std::env::var("CARGO_PKG_VERSION").expect("Cargo package version"));
    println!("cargo:rustc-env=FUJIN_BUILD_VERSION={version}");
}
