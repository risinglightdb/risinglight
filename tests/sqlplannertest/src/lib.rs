// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use anyhow::Error;
use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::Database;
use sqlplannertest::ParsedTestCase;

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
                .map(datachunk_to_sqllogictest_string)
                .collect();
            Ok(output)
        } else {
            Ok(String::new())
        }
    }
}
