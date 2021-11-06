use criterion::*;
use risinglight::array::I32Array;

fn array_mul(c: &mut Criterion) {
    let mut group = c.benchmark_group("array mul");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::executor::evaluator;
            let a1: I32Array = (0..size).collect();
            let a2: I32Array = (0..size).collect();
            b.iter(|| {
                #[cfg(not(feature = "simd"))]
                let _: I32Array = evaluator::binary_op(&a1, &a2, |a, b| a * b);
                #[cfg(feature = "simd")]
                let _: I32Array = evaluator::simd_op::<_, _, _, 32>(&a1, &a2, |a, b| a * b);
            });
        });
    }
    group.finish();
}

fn array_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_sum");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536, 1048576] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            #[cfg(not(feature = "simd"))]
            use risinglight::{array::Array, executor::sum_i32};
            let a1: I32Array = (0..size).collect();
            b.iter(|| {
                #[cfg(not(feature = "simd"))]
                a1.iter().fold(None, sum_i32);
                #[cfg(feature = "simd")]
                a1.batch_iter::<32>().sum::<i32>();
            })
        });
    }
    group.finish();
}
criterion_group!(benches, array_mul, array_sum);
criterion_main!(benches);
