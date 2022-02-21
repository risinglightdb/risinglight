// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::env;
use std::fmt::Write as _;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

pub fn add_env_contributors() {
    let output = Command::new("git")
        .arg("shortlog")
        .arg("HEAD")
        .arg("-s")
        .output()
        .unwrap();
    let authors: String = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|s| {
            let mut s = s[7..].to_owned();
            s.push(',');
            s
        })
        .collect();
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rustc-env=RISINGLIGHT_CONTRIBUTORS={}", authors);
}

fn main() {
    add_env_contributors();
    // Scan test scripts and generate test cases.
    println!("cargo:rerun-if-changed=tests/sql");
    const PATTERN: &str = "tests/sql/**/[!_]*.slt"; // ignore files start with '_'
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt"];
    const DISK_BLOCKLIST: &[&str] = &[];

    let path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("testcase.rs");
    let mut fout = std::fs::File::create(path).expect("failed to create file");

    let paths = glob::glob(PATTERN).expect("failed to find test files");
    let mut mem_attrs = String::new();
    let mut disk_attrs = String::new();
    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        let subpath = path.strip_prefix("tests/sql").unwrap().to_str().unwrap();
        if !MEM_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            writeln!(mem_attrs, "#[test_case::test_case({:?})]", subpath).unwrap();
        }
        if !DISK_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            writeln!(disk_attrs, "#[test_case::test_case({:?})]", subpath).unwrap();
        }
    }
    writeln!(
        fout,
        "{mem_attrs}
        fn mem(name: &str) {{ test_mem(name); }}
        {disk_attrs}
        fn disk(name: &str) {{ test_disk(name); }}"
    )
    .expect("failed to write file");
}
