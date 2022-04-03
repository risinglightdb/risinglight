// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::Write as _;
use std::io::Write;
use std::path::PathBuf;

fn main() {
    // Scan test scripts and generate test cases.
    println!("cargo:rerun-if-changed=../../tests/sql");

    const PATTERN: &str = "../../tests/sql/**/[!_]*.slt"; // ignore files start with '_'
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt", "join.slt"];
    const DISK_BLOCKLIST: &[&str] = &["join.slt"];

    let path = PathBuf::from("tests").join("gen/testcase.rs");
    let mut fout = std::fs::File::create(path).expect("failed to create file");

    let paths = glob::glob(PATTERN).expect("failed to find test files");
    let mut mem_attrs = String::new();
    let mut disk_attrs = String::new();
    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        let subpath = path
            .strip_prefix("../../tests/sql")
            .unwrap()
            .to_str()
            .unwrap();
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
#[tokio::test(flavor = \"multi_thread\")]
async fn mem(name: &str) {{ risinglight_sqllogictest::test_mem(name).await; }}

{disk_attrs}
#[tokio::test(flavor = \"multi_thread\")]
async fn disk(name: &str) {{ risinglight_sqllogictest::test_disk(name).await; }}"
    )
    .expect("failed to write file");
}
