// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight sqllogictest

use std::path::Path;

use libtest_mimic::{Arguments, Trial};
use risinglight_sqllogictest::{test, Engine};
use tokio::runtime::Runtime;

fn main() {
    init_logger();

    const PATTERN: &str = "tests/sql/**/[!_]*.slt"; // ignore files start with '_'
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt"];
    const DISK_BLOCKLIST: &[&str] = &[];

    let mut tests = vec![];

    for version in ["v1", "v2"] {
        let v1 = version == "v1";
        let paths = glob::glob(PATTERN).expect("failed to find test files");
        for entry in paths {
            let path = entry.expect("failed to read glob entry");
            let subpath = path.strip_prefix("tests/sql").unwrap().to_str().unwrap();
            if !MEM_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
                let path = path.clone();
                let engine = Engine::Mem;
                tests.push(Trial::test(
                    format!("{}::{}::{}", version, engine, subpath),
                    move || Ok(build_runtime().block_on(test(&path, engine, v1))?),
                ));
            }
            if !DISK_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
                let engine = Engine::Disk;
                tests.push(Trial::test(
                    format!("{}::{}::{}", version, engine, subpath),
                    move || Ok(build_runtime().block_on(test(&path, engine, v1))?),
                ));
            }
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

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::init();
        // Force set pwd to the root directory of RisingLight
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
        std::env::set_current_dir(path).unwrap();
    });
}
