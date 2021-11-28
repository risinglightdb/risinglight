use crate::{Database, Error};
use sqllogictest::SqlLogicTester;
use std::path::Path;
use test_case::test_case;

#[test_case("01-01.slt")]
fn test(name: &str) {
    init_logger();
    let script = std::fs::read_to_string(Path::new("../sql").join(name)).unwrap();
    let mut tester = SqlLogicTester::new(Database::new());
    tester.test_script(&script);
}

impl sqllogictest::DB for Database {
    type Error = Error;
    fn run(&self, sql: &str) -> Result<Vec<String>, Self::Error> {
        self.run(sql)
    }
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(env_logger::init);
}
