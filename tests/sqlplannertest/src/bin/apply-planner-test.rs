use std::path::Path;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    sqlplannertest::planner_test_apply(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("planner_test"),
        || async { Ok(risinglight_plannertest::DatabaseWrapper::default()) },
    )
    .await?;
    Ok(())
}
