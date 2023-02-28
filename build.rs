use std::env;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::io::{self, Write};

fn main() {
    exe_exists("make").expect("Can't find make; perhaps install with: apt install make");
    exe_exists("cc").expect("Can't find cc: perhaps install with: apt install gcc");
    let out_dir = env::var("OUT_DIR").unwrap();
    let output = std::process::Command::new("make")
        .args(&[format!("{}/libaio.a", out_dir)])
        .env("OUT_DIR", &out_dir)
        .current_dir("./libaio")
        .output()
        .expect("make failed");
    if !output.status.success() {
        io::stderr().write_all(&output.stderr).unwrap();
        panic!("make failed");
    }
    // the current source version of libaio is 0.3.112
    println!("cargo:rerun-if-changed=libaio");
    println!("cargo:rustc-link-search=native={}", out_dir);
}

fn exe_exists(exe: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_else(|| OsStr::new("").into());
    std::env::split_paths(&path).find(|path| path.join(exe).exists())
}
