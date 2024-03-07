// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use anyhow::{Error, Result};
use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::Database;
use sqlplannertest::ParsedTestCase;

#[tokio::main]
async fn main() -> Result<()> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/planner_test");
    if std::env::var("UPDATE_PLANNER").is_ok() {
        sqlplannertest::planner_test_apply(path, || async { Ok(DatabaseWrapper) }).await?;
    } else {
        sqlplannertest::planner_test_runner(path, || async { Ok(DatabaseWrapper) })?;
    }
    Ok(())
}

#[derive(Default)]
pub struct DatabaseWrapper;

#[async_trait::async_trait]
impl sqlplannertest::PlannerTestRunner for DatabaseWrapper {
    async fn run(&mut self, test_case: &ParsedTestCase) -> Result<String, Error> {
        if !test_case.tasks.is_empty() {
            let db = Database::new_on_disk(SecondaryStorageOptions::default_for_test()).await;
            for sql in &test_case.before_sql {
                db.run(sql).await?;
            }
            let chunks = db.run(&test_case.sql).await?;
            let output = chunks
                .iter()
                .map(|c| {
                    let rows = datachunk_to_sqllogictest_string(c);
                    rows.into_iter()
                        .map(|row| row.join("\t"))
                        .collect::<Vec<String>>()
                        .join("\n")
                })
                .collect();
            Ok(output)
        } else {
            Ok(String::new())
        }
    }
}
