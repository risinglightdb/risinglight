// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight sqllogictest

use libtest_mimic::{run_tests, Arguments, Outcome, Test};
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
            tests.push(Test {
                name: format!(
                    "mem_{}",
                    subpath.strip_suffix(".slt").unwrap().replace('/', "_")
                ),
                kind: "".into(),
                is_ignored: false,
                is_bench: false,
                data: ("mem", subpath.to_string()),
            });
        }
        if !DISK_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            tests.push(Test {
                name: format!(
                    "disk_{}",
                    subpath.strip_suffix(".slt").unwrap().replace('/', "_")
                ),
                kind: "".into(),
                is_ignored: false,
                is_bench: false,
                data: ("disk", subpath.to_string()),
            });
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

    run_tests(&args, tests, |test| match &test.data {
        ("mem", case) => {
            build_runtime().block_on(test_mem(case));
            Outcome::Passed
        }
        ("disk", case) => {
            build_runtime().block_on(test_disk(case));
            Outcome::Passed
        }
        _ => unreachable!(),
    })
    .exit();
}
