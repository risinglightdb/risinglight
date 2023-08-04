// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use criterion::*;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::Database;

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_tpch
}
criterion_main!(benches);

fn bench_tpch(c: &mut Criterion) {
    let db_dir = std::path::Path::new("target/bench-tpch.db");
    let create_sql = std::fs::read_to_string("tests/sql/tpch/create.sql").unwrap();
    let import_sql = std::fs::read_to_string("tests/sql/tpch/import.sql").unwrap();
    let should_import = !db_dir.exists();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(async {
        let opt = SecondaryStorageOptions {
            path: db_dir.into(),
            ..SecondaryStorageOptions::default_for_cli()
        };
        let db = Database::new_on_disk(opt).await;
        if should_import {
            db.run(&create_sql).await.unwrap();
            db.run(&import_sql).await.unwrap();
        }
        db
    });
    for num in 1..=22 {
        let name = format!("explain-q{num}");
        let path = PathBuf::from(format!("tests/sql/tpch/q{num}.sql"));
        if !path.exists() {
            continue;
        }
        let sql = format!("explain {}", std::fs::read_to_string(&path).unwrap());
        c.bench_function(&name, |b| b.to_async(&rt).iter(|| db.run(&sql)));
    }
    for num in 1..=22 {
        let name = format!("run-q{num}");
        let path = PathBuf::from(format!("tests/sql/tpch/q{num}.sql"));
        if !path.exists() {
            continue;
        }
        let sql = std::fs::read_to_string(&path).unwrap();
        c.bench_function(&name, |b| b.to_async(&rt).iter(|| db.run(&sql)));
    }
}
