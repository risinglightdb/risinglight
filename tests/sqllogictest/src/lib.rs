// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};

async fn test(db: Database, name: &str) {
    let db = DatabaseWrapper(db);
    let mut tester = sqllogictest::Runner::new(&db);
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
    db.0.shutdown().await.unwrap();
}

pub async fn test_mem(name: &str) {
    init_logger();
    let db = Database::new_in_memory();
    test(db, name).await;
}

pub async fn test_disk(name: &str) {
    init_logger();
    let db = Database::new_on_disk(SecondaryStorageOptions::default_for_test()).await;
    test(db, name).await;
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

/// New type to implement sqllogictest driver trait for risinglight.
struct DatabaseWrapper(Database);

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for &DatabaseWrapper {
    type Error = Error;
    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        let chunks = self.0.run(sql).await?;
        let output = chunks
            .iter()
            .map(datachunk_to_sqllogictest_string)
            .collect();
        Ok(output)
    }
}
