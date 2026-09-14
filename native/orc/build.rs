fn main() {
    streamfusion_native_build::configure();
    println!("cargo:rerun-if-changed=cpp");
    println!("cargo:rerun-if-changed=cmake.py");
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    // Cargo feature combinations change build-script hashes but not this C++ ABI. Reuse the
    // CMake tree within the locked target/profile directory instead of rebuilding its dependencies.
    let cpp_out = out.ancestors().nth(3).unwrap().join("orc-cpp");
    if std::env::var_os("CMAKE").is_none() {
        let output = std::process::Command::new("python3")
            .arg("cmake.py")
            .arg(cpp_out.parent().unwrap().join("build-tools"))
            .output()
            .expect("ORC builds need Python 3 to locate or bootstrap CMake");
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success(), "Unable to prepare CMake for ORC");
        std::env::set_var("CMAKE", String::from_utf8(output.stdout).unwrap().trim());
    }
    let mut build = cmake::Config::new("cpp");
    build.out_dir(cpp_out);
    build.define("CMAKE_INSTALL_LIBDIR", "lib");
    let target = std::env::var("TARGET").unwrap();
    if target.contains("apple-darwin") {
        let host = std::env::var("HOST").unwrap();
        build.define(
            "SF_HOST_ARCH",
            if host.starts_with("aarch64") {
                "arm64"
            } else {
                "x86_64"
            },
        );
        build.define(
            "CMAKE_OSX_ARCHITECTURES",
            if target.starts_with("aarch64") {
                "arm64"
            } else {
                "x86_64"
            },
        );
    }
    let output = build.build();
    println!("cargo:rustc-link-search=native={}/lib", output.display());
    for library in [
        "streamfusion_orc_cpp",
        "orc",
        "nanoarrow_static",
        "orc_vendored_protobuf",
        "orc_vendored_snappy",
        "orc_vendored_zlib",
        "orc_vendored_zstd",
        "orc_vendored_lz4",
    ] {
        println!("cargo:rustc-link-lib=static={library}");
    }
    println!(
        "cargo:rustc-link-lib={}",
        if target.contains("apple") {
            "c++"
        } else {
            "stdc++"
        }
    );
}
