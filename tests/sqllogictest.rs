// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};
use tempfile::tempdir;
use test_case::test_case;
use tokio::runtime::Runtime;

#[test_case("basic_test.slt")]
#[test_case("operator.slt")]
#[test_case("nullable_and_or_eval.slt")]
#[test_case("filter.slt")]
#[test_case("order_by.slt")]
#[test_case("create.slt")]
#[test_case("insert.slt")]
#[test_case("select.test")]
#[test_case("join.slt")]
#[test_case("limit.slt")]
#[test_case("type.slt")]
#[test_case("aggregation.slt")]
#[test_case("delete.slt")]
#[test_case("copy/csv.slt")]
#[test_case("where.slt")]
#[test_case("tpch/tpch.slt")]
#[test_case("catalog.slt")]
// #[test_case("select.slt")]
// #[test_case("issue_347.slt")]
fn test_mem(name: &str) {
    init_logger();
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper {
        rt: Runtime::new().unwrap(),
        db: Database::new_in_memory(),
    });
    tester.enable_testdir();
    tester
        .run_file(Path::new("tests/sql").join(name))
        .map_err(|e| panic!("{}", e))
        .unwrap();
}

#[test_case("basic_test.slt")]
#[test_case("operator.slt")]
#[test_case("nullable_and_or_eval.slt")]
#[test_case("filter.slt")]
#[test_case("order_by.slt")]
#[test_case("create.slt")]
#[test_case("insert.slt")]
#[test_case("select.test")]
#[test_case("join.slt")]
#[test_case("limit.slt")]
#[test_case("type.slt")]
#[test_case("aggregation.slt")]
#[test_case("delete.slt")]
#[test_case("copy/csv.slt")]
#[test_case("where.slt")]
#[test_case("tpch/tpch.slt")]
#[test_case("statistics.slt")]
#[test_case("catalog.slt")]
// #[test_case("select.slt")]
// #[test_case("issue_347.slt")]
fn test_disk(name: &str) {
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
        .map_err(|e| panic!("{}", e))
        .unwrap();
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
    fn run(&self, sql: &str) -> Result<String, Self::Error> {
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
