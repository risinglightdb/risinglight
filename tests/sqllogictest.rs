// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::thread::available_parallelism;

use futures::{Future, StreamExt};
use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn run_all_test_files() {
    const PATTERN: &str = "tests/sql/**/[!_]*.slt"; // ignore files start with '_'
    const MEM_BLOCKLIST: &[&str] = &["statistics.slt"];
    const DISK_BLOCKLIST: &[&str] = &[];

    let paths = glob::glob(PATTERN).expect("failed to find test files");

    let mut test_cases: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![];

    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        let subpath = path
            .strip_prefix("tests/sql")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        if !MEM_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            let path = path.clone();
            let subpath = subpath.clone();
            test_cases.push(Box::pin(async move {
                println!("-- running {} (mem) --", path.to_str().unwrap());
                test_mem(&subpath).await
            }));
        }
        if !DISK_BLOCKLIST.iter().any(|p| subpath.contains(p)) {
            let path = path.clone();
            let subpath = subpath.clone();
            test_cases.push(Box::pin(async move {
                println!("-- running {} (disk) --", path.to_str().unwrap());
                test_disk(&subpath).await
            }));
        }
    }

    let stream =
        futures::stream::iter(test_cases).buffer_unordered(available_parallelism().unwrap().get());

    stream.count().await;
}

async fn test_mem(name: &str) {
    init_logger();
    let db = Arc::new(Database::new_in_memory());
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper { db: db.clone() });
    tester.enable_testdir();
    tester
        .run_file_async(Path::new("tests/sql").join(name))
        .await
        .unwrap();
    db.shutdown().await.unwrap();
}

async fn test_disk(name: &str) {
    init_logger();
    let temp_dir = tempdir().unwrap();
    let db = Database::new_on_disk(SecondaryStorageOptions::default_for_test(
        temp_dir.path().to_path_buf(),
    ))
    .await;
    let db = Arc::new(db);
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper { db: db.clone() });
    tester.enable_testdir();
    tester
        .run_file_async(Path::new("tests/sql").join(name))
        .await
        .unwrap();
    db.shutdown().await.unwrap();
}

fn init_logger() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(env_logger::init);
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
