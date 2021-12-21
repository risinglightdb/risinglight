use std::path::Path;

use test_case::test_case;

use crate::array::DataChunk;
use crate::types::DataValue;
use crate::{Database, Error};

#[test_case("01-01.slt")]
#[test_case("01-03.slt")]
#[test_case("01-05.slt")]
#[test_case("01-06.slt")]
#[test_case("01-07.slt")]
fn test(name: &str) {
    init_logger();
    let script = std::fs::read_to_string(Path::new("../sql").join(name)).unwrap();
    let mut tester = sqllogictest::Runner::new(Database::new());
    tester.run_script(&script);
}

impl sqllogictest::DB for Database {
    type Error = Error;
    fn run(&self, sql: &str) -> Result<String, Self::Error> {
        let chunks = self.run(sql)?;
        let strings = chunks.iter().map(datachunk_to_string).collect();
        Ok(strings)
    }
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(env_logger::init);
}

fn datachunk_to_string(chunk: &DataChunk) -> String {
    use std::fmt::Write;
    let mut string = String::new();
    for row in 0..chunk.cardinality() {
        for (col, array) in chunk.arrays().iter().enumerate() {
            if col != 0 {
                write!(string, " ").unwrap();
            }
            match array.get(row) {
                DataValue::Null => write!(string, "NULL"),
                DataValue::Bool(v) => write!(string, "{}", v),
                DataValue::Int32(v) => write!(string, "{}", v),
                DataValue::Float64(v) => write!(string, "{}", v),
                DataValue::String(s) if s.is_empty() => write!(string, "(empty)"),
                DataValue::String(s) => write!(string, "{}", s),
            }
            .unwrap();
        }
        writeln!(string).unwrap();
    }
    string
}
