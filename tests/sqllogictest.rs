// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};
use tempfile::tempdir;
use tokio::runtime::Runtime;

#[test]
fn run_all_test_files() {
    const PATTERN: &str = "tests/sql/**/[!_]*.slt"; // ignore files start with '_'
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt"];
    const DISK_BLOCKLIST: &[&str] = &[];

    let paths = glob::glob(PATTERN).expect("failed to find test files");
    let mut pass = true;
    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        let subpath = path.strip_prefix("tests/sql").unwrap().to_str().unwrap();
        if !MEM_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            println!("-- running {} (mem) --", path.to_str().unwrap());
            pass = pass & test_mem(subpath);
        }
        if !DISK_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            println!("-- running {} (disk) --", path.to_str().unwrap());
            pass = pass & test_disk(subpath);
        }
    }
    assert!(pass);
}

fn test_mem(name: &str) ->bool {
    init_logger();
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper {
        rt: Runtime::new().unwrap(),
        db: Database::new_in_memory(),
    });
    tester.enable_testdir();
    tester
        .run_file(Path::new("tests/sql").join(name))
        .map_err(|e| println!("{}", e))
        .is_ok()
}

fn test_disk(name: &str) ->bool {
    init_logger();
    let temp_dir = tempdir().unwrap();
    let rt = Runtime::new().unwrap();
    let db = rt.block_on(Database::new_on_disk(
        SecondaryStorageOptions::default_for_test(temp_dir.path().to_path_buf()),
    ));
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper { rt, db });
    tester.enable_testdir();
    tester
        .run_file(Path::new("tests/sql").join(name))
        .map_err(|e| println!("{}", e))
        .is_ok()
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(env_logger::init);
}

struct DatabaseWrapper {
    rt: Runtime,
    db: Database,
}

impl sqllogictest::DB for DatabaseWrapper {
    type Error = Error;
    fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        let chunks = self.rt.block_on(self.db.run(sql))?;
        let output = chunks
            .iter()
            .map(datachunk_to_sqllogictest_string)
            .collect();
        Ok(output)
    }
}

impl Drop for DatabaseWrapper {
    fn drop(&mut self) {
        self.rt.block_on(self.db.shutdown()).unwrap();
    }
}
