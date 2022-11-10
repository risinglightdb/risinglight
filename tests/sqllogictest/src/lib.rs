// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt::Display;
use std::path::Path;

use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};

type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
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

pub async fn test(filename: impl AsRef<Path>, engine: Engine, v1: bool) -> Result<()> {
    let db = match engine {
        Engine::Disk => Database::new_on_disk(SecondaryStorageOptions::default_for_test()).await,
        Engine::Mem => Database::new_in_memory(),
    };
    if v1 {
        db.run_internal("v1").await.unwrap();
    }

    let db = DatabaseWrapper(db);
    let mut tester = sqllogictest::Runner::new(&db);
    tester.enable_testdir();
    tester.run_file_async(filename).await?;
    db.0.shutdown().await?;
    Ok(())
}

/// New type to implement sqllogictest driver trait for risinglight.
struct DatabaseWrapper(Database);

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for &DatabaseWrapper {
    type Error = Error;
    async fn run(&mut self, sql: &str) -> core::result::Result<String, Self::Error> {
        let chunks = self.0.run(sql).await?;
        let output = chunks
            .iter()
            .map(datachunk_to_sqllogictest_string)
            .collect();
        Ok(output)
    }
}
