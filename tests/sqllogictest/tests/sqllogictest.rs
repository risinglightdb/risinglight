// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight sqllogictest

use std::env;

use libtest_mimic::{Arguments, Trial};
use risinglight_sqllogictest::{test, Engine};
use tokio::runtime::Runtime;

fn main() {
    const PATTERN: &str = "../sql/**/[!_]*.slt"; // ignore files start with '_'
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt"];
    const DISK_BLOCKLIST: &[&str] = &[];

    let current_dir = env::current_dir().unwrap();

    let paths = glob::glob(PATTERN).expect("failed to find test files");

    let mut tests = vec![];

    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        let subpath = path.strip_prefix("../sql").unwrap().to_str().unwrap();
        let path = current_dir.join(path.clone());
        if !MEM_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            let path = path.clone();
            let engine = Engine::Mem;
            tests.push(Trial::test(
                format!("{}::{}", engine, subpath.to_string()),
                move || Ok(build_runtime().block_on(test(&path, engine))?),
            ));
        }
        if !DISK_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            let engine = Engine::Disk;
            tests.push(Trial::test(
                format!("{}::{}", engine, subpath.to_string()),
                move || Ok(build_runtime().block_on(test(&path, engine))?),
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

    libtest_mimic::run(&Arguments::from_args(), tests).exit();
}
