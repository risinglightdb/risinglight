// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::env;
use std::path::Path;

use libtest_mimic::{Arguments, Trial};
use risinglight::array::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::{Database, Error};
use tokio::runtime::Runtime;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
pub enum Engine {
    Disk,
    Mem,
}

pub fn run(blocklist: &[&str], engine: Engine) {
    const PATTERN: &str = "../sql/**/[!_]*.slt"; // ignore files start with '_'
    let current_dir = env::current_dir().unwrap();

    let paths = glob::glob(PATTERN).expect("failed to find test files");

    let mut tests = vec![];

    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        let subpath = path.strip_prefix("../sql").unwrap().to_str().unwrap();
        let path = current_dir.join(path.clone());
        if !blocklist.iter().any(|p| subpath.contains(p)) {
            tests.push(Trial::test(format!("{subpath}"), move || {
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

async fn test(filename: impl AsRef<Path>, engine: Engine) -> Result<()> {
    init_logger();

    let db = match engine {
        Engine::Disk => Database::new_on_disk(SecondaryStorageOptions::default_for_test()).await,
        Engine::Mem => Database::new_in_memory(),
    };

    let db = DatabaseWrapper(db);
    let mut tester = sqllogictest::Runner::new(&db);
    tester.enable_testdir();
    tester.run_file_async(filename).await?;
    db.0.shutdown().await?;
    Ok(())
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
    async fn run(&mut self, sql: &str) -> core::result::Result<String, Self::Error> {
        let chunks = self.0.run(sql).await?;
        let output = chunks
            .iter()
            .map(datachunk_to_sqllogictest_string)
            .collect();
        Ok(output)
    }
}
