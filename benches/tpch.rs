// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

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
    let queries = [1, 3, 5, 6, 9, 10];
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
    for q in queries {
        let query = format!("q{q}");
        let query_sql = std::fs::read_to_string(format!("tests/sql/tpch/{query}.sql")).unwrap();
        c.bench_function(&query, |b| b.to_async(&rt).iter(|| db.run(&query_sql)));
    }
}
