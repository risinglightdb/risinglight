// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Sqllogictest for RisingLight.

use std::fmt::Display;
use std::path::Path;

use libtest_mimic::{Arguments, Trial};
use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};
use sqllogictest::{DBOutput, DefaultColumnType};
use tokio::runtime::Runtime;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

fn main() {
    tracing_subscriber::fmt::init();

    const PATTERN: &str = "tests/sql/**/[!_]*.slt"; // ignore files start with '_'
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt"];
    const DISK_BLOCKLIST: &[&str] = &[];

    let mut tests = vec![];

    let paths = glob::glob(PATTERN).expect("failed to find test files");
    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        let subpath = path.strip_prefix("tests/sql").unwrap().to_str().unwrap();
        if !MEM_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            let path = path.clone();
            let engine = Engine::Mem;
            tests.push(Trial::test(format!("{}::{}", engine, subpath), move || {
                Ok(build_runtime().block_on(test(&path, engine))?)
            }));
        }
        if !DISK_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            let engine = Engine::Disk;
            tests.push(Trial::test(format!("{}::{}", engine, subpath), move || {
                Ok(build_runtime().block_on(test(&path, engine))?)
            }));
        }
    }

    if tests.is_empty() {
        panic!(
            "no test found for sqllogictest! pwd: {:?}",
            std::env::current_dir().unwrap()
        );
    }

    fn build_runtime() -> Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    libtest_mimic::run(&Arguments::from_args(), tests).exit();
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Engine {
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

async fn test(filename: impl AsRef<Path>, engine: Engine) -> Result<()> {
    let db = match engine {
        Engine::Disk => Database::new_on_disk(SecondaryStorageOptions::default_for_test()).await,
        Engine::Mem => Database::new_in_memory(),
    };

    let db = DatabaseWrapper(db);
    let mut tester = sqllogictest::Runner::new(|| async { Ok(&db) });

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
