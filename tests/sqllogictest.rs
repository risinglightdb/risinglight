use std::path::Path;

use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::types::DataValue;
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
// #[test_case("select.slt")]
// #[test_case("issue_347.slt")]
fn test_mem(name: &str) {
    init_logger();
    let script = std::fs::read_to_string(Path::new("tests/sql").join(name)).unwrap();
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper {
        rt: Runtime::new().unwrap(),
        db: Database::new_in_memory(),
    });
    tester.enable_testdir();
    tester.run_script(&script);
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
// #[test_case("select.slt")]
// #[test_case("issue_347.slt")]
fn test_disk(name: &str) {
    init_logger();
    let temp_dir = tempdir().unwrap();
    let script = std::fs::read_to_string(Path::new("tests/sql").join(name)).unwrap();
    let rt = Runtime::new().unwrap();
    let db = rt.block_on(Database::new_on_disk(
        SecondaryStorageOptions::default_for_test(temp_dir.path().to_path_buf()),
    ));
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper { rt, db });
    tester.enable_testdir();
    tester.run_script(&script);
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
        let output = chunks.iter().map(datachunk_to_string).collect();
        Ok(output)
    }
}

impl Drop for DatabaseWrapper {
    fn drop(&mut self) {
        self.rt.block_on(self.db.shutdown()).unwrap();
    }
}

fn datachunk_to_string(chunk: &DataChunk) -> String {
    let mut output = String::new();
    for row in 0..chunk.cardinality() {
        use std::fmt::Write;
        for (col, array) in chunk.arrays().iter().enumerate() {
            if col != 0 {
                write!(output, " ").unwrap();
            }
            match array.get(row) {
                DataValue::Null => write!(output, "NULL"),
                DataValue::Bool(v) => write!(output, "{}", v),
                DataValue::Int32(v) => write!(output, "{}", v),
                DataValue::Int64(v) => write!(output, "{}", v),
                DataValue::Float64(v) => write!(output, "{}", v),
                DataValue::String(s) if s.is_empty() => write!(output, "(empty)"),
                DataValue::String(s) => write!(output, "{}", s),
                DataValue::Decimal(v) => write!(output, "{}", v),
                DataValue::Date(v) => write!(output, "{}", v),
            }
            .unwrap();
        }
        writeln!(output).unwrap();
    }
    output
}
