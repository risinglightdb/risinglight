// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight sqllogictest

use libtest_mimic::{Arguments, Trial};
use risinglight_sqllogictest::{test_disk, test_mem};
use tokio::runtime::Runtime;

fn main() {
    const PATTERN: &str = "../sql/**/[!_]*.slt"; // ignore files start with '_'
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt"];
    const DISK_BLOCKLIST: &[&str] = &[];

    let paths = glob::glob(PATTERN).expect("failed to find test files");

    let args = Arguments::from_args();
    let mut tests = vec![];

    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        let subpath = path.strip_prefix("../sql").unwrap().to_str().unwrap();
        if !MEM_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            let subpath = subpath.to_owned();
            tests.push(Trial::test(
                format!(
                    "mem_{}",
                    subpath.strip_suffix(".slt").unwrap().replace('/', "_")
                ),
                move || Ok(build_runtime().block_on(test_mem(&subpath))),
            ));
        }
        if !DISK_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            let subpath = subpath.to_owned();
            tests.push(Trial::test(
                format!(
                    "disk_{}",
                    subpath.strip_suffix(".slt").unwrap().replace('/', "_")
                ),
                move || Ok(build_runtime().block_on(test_disk(&subpath))),
            ));
        }
    }

    if tests.is_empty() {
        panic!(
            "no test found for sqllogictest! pwd: {:?}",
            std::env::current_dir().unwrap()
        );
    }

    fn build_runtime() -> Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    libtest_mimic::run(&args, tests).exit();
}
