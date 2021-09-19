use criterion::*;
use risinglight::Database;

fn create_table(c: &mut Criterion) {
    c.bench_function("create table", |b| {
        b.iter_batched(
            Database::new,
            |db| {
                db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
            },
            BatchSize::LargeInput,
        );
    });
}

fn insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let sql = std::iter::once("insert into t values ")
                .chain(std::iter::repeat("(1,10,100),").take(size - 1))
                .chain(std::iter::once("(1,10,100)"))
                .collect::<String>();
            b.iter_batched(
                || {
                    let db = Database::new();
                    db.run("create table t(v1 int, v2 int, v3 int)").unwrap();
                    db
                },
                |db| {
                    db.run(&sql).unwrap();
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, create_table, insert);
criterion_main!(benches);
