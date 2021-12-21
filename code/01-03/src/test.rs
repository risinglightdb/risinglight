use std::path::Path;

use test_case::test_case;

use crate::{Database, Error};

#[test_case("01-03.slt")]
fn test(name: &str) {
    init_logger();
    let script = std::fs::read_to_string(Path::new("../sql").join(name)).unwrap();
    let mut tester = sqllogictest::Runner::new(Database::new());
    tester.run_script(&script);
}

impl sqllogictest::DB for Database {
    type Error = Error;
    fn run(&self, sql: &str) -> Result<String, Self::Error> {
        let mut outputs = self.run(sql)?;
        Ok(outputs.remove(0))
    }
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(env_logger::init);
}
