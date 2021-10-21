use criterion::*;
use risinglight::Database;

fn create_table(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("create table", |b| {
        b.to_async(&runtime).iter_batched(
            Database::new_in_memory,
            |db| async move {
                db.run("create table t(v1 int, v2 int, v3 int)")
                    .await
                    .unwrap()
            },
            BatchSize::LargeInput,
        );
    });
}

fn insert(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("insert");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let sql = std::iter::once("insert into t values ")
                .chain(std::iter::repeat("(1,10,100),").take(size - 1))
                .chain(std::iter::once("(1,10,100)"))
                .collect::<String>();
            b.to_async(&runtime).iter_batched(
                || async {
                    let db = Database::new_in_memory();
                    db.run("create table t(v1 int, v2 int, v3 int)")
                        .await
                        .unwrap();
                    db
                },
                |db| async {
                    db.await.run(&sql).await.unwrap();
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, create_table, insert);
criterion_main!(benches);
