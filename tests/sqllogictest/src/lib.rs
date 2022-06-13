// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::Arc;

use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};

pub async fn test_mem(name: &str) {
    init_logger();
    let db = Arc::new(Database::new_in_memory());
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper { db: db.clone() });
    tester.enable_testdir();

    tester
        .run_file_async(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("sql")
                .join(name),
        )
        .await
        .unwrap();
    db.shutdown().await.unwrap();
}

pub async fn test_disk(name: &str) {
    init_logger();
    let db = Database::new_on_disk(SecondaryStorageOptions::default_for_test()).await;
    let db = Arc::new(db);
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper { db: db.clone() });
    tester.enable_testdir();
    tester
        .run_file_async(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("sql")
                .join(name),
        )
        .await
        .unwrap();
    db.shutdown().await.unwrap();
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::init();
        // Force set pwd to the root directory of RisingLight
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
        println!("{:?}", path);
        std::env::set_current_dir(&path).unwrap();
    });
}

struct DatabaseWrapper {
    db: Arc<Database>,
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for DatabaseWrapper {
    type Error = Error;
    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        let chunks = self.db.run(sql).await?;
        let output = chunks
            .iter()
            .map(datachunk_to_sqllogictest_string)
            .collect();
        Ok(output)
    }
}
