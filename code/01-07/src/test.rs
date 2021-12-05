use crate::{array::DataChunk, types::DataValue, Database, Error};
use sqllogictest::SqlLogicTester;
use std::path::Path;
use test_case::test_case;

#[test_case("01-03.slt")]
#[test_case("01-05.slt")]
#[test_case("01-06.slt")]
#[test_case("01-07.slt")]
fn test(name: &str) {
    init_logger();
    let script = std::fs::read_to_string(Path::new("../sql").join(name)).unwrap();
    let mut tester = SqlLogicTester::new(Database::new());
    tester.test_script(&script);
}

impl sqllogictest::DB for Database {
    type Error = Error;
    fn run(&self, sql: &str) -> Result<Vec<String>, Self::Error> {
        let chunks = self.run(sql)?;
        let strings = chunks.iter().map(datachunk_to_strings).flatten().collect();
        Ok(strings)
    }
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(env_logger::init);
}

fn datachunk_to_strings(chunk: &DataChunk) -> Vec<String> {
    let mut lines = vec![];
    for row in 0..chunk.cardinality() {
        let mut line = String::new();
        for (col, array) in chunk.arrays().iter().enumerate() {
            use std::fmt::Write;
            if col != 0 {
                write!(line, " ").unwrap();
            }
            match array.get(row) {
                DataValue::Null => write!(line, "NULL"),
                DataValue::Bool(v) => write!(line, "{}", v),
                DataValue::Int32(v) => write!(line, "{}", v),
                DataValue::Float64(v) => write!(line, "{}", v),
                DataValue::String(s) if s.is_empty() => write!(line, "(empty)"),
                DataValue::String(s) => write!(line, "{}", s),
            }
            .unwrap();
        }
        lines.push(line);
    }
    lines
}
