use std::path::Path;

use anyhow::Result;

fn main() -> Result<()> {
    sqlplannertest::planner_test_runner(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("planner_test"),
        || async { Ok(risinglight_plannertest::DatabaseWrapper::default()) },
    )?;
    Ok(())
}
