// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::Display;
use std::path::Path;

use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};
use sqllogictest::{DBOutput, DefaultColumnType};

type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Disk,
    Mem,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Disk => f.write_str("disk"),
            Engine::Mem => f.write_str("mem"),
        }
    }
}

pub async fn test(filename: impl AsRef<Path>, engine: Engine) -> Result<()> {
    let db = match engine {
        Engine::Disk => Database::new_on_disk(SecondaryStorageOptions::default_for_test()).await,
        Engine::Mem => Database::new_in_memory(),
    };

    let db = DatabaseWrapper(db);
    let mut tester = sqllogictest::Runner::new(&db);
    tester.enable_testdir();

    // Uncomment the following lines to update the test files.
    // if engine == Engine::Disk {
    //     // Only use one engine to update to avoid conflicts.
    //     sqllogictest::update_test_file(filename, tester, "\t", sqllogictest::default_validator)
    //         .await?;
    // }

    tester.run_file_async(filename).await?;
    db.0.shutdown().await?;
    Ok(())
}

/// New type to implement sqllogictest driver trait for risinglight.
struct DatabaseWrapper(Database);

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for &DatabaseWrapper {
    type ColumnType = DefaultColumnType;

    type Error = Error;
    async fn run(
        &mut self,
        sql: &str,
    ) -> core::result::Result<DBOutput<DefaultColumnType>, Self::Error> {
        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };

        let chunks = self.0.run(sql).await?;
        if chunks.is_empty() || chunks.iter().all(|c| c.data_chunks().is_empty()) {
            if is_query_sql {
                return Ok(DBOutput::Rows {
                    types: vec![],
                    rows: vec![],
                });
            } else {
                return Ok(DBOutput::StatementComplete(0));
            }
        }
        let types = vec![DefaultColumnType::Any; chunks[0].get_first_data_chunk().column_count()];
        let rows = chunks
            .iter()
            .flat_map(datachunk_to_sqllogictest_string)
            .collect();
        Ok(DBOutput::Rows { types, rows })
    }
}
