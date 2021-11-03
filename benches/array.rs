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
    for size in [1, 16, 256, 4096, 65536, 131072, 262144, 524288, 1048576] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            #[cfg(feature = "simd")]
            use risinglight::array::ArraySIMDSum;
            #[cfg(not(feature = "simd"))]
            use risinglight::executor::evaluator;
            let a1: I32Array = (0..size).collect();
            b.iter(|| {
                #[cfg(not(feature = "simd"))]
                {
                    let temp: Option<i32> = None;
                    a1.iter().fold(temp, sum_i32);
                }
                #[cfg(feature = "simd")]
                a1.simd_sum();
            })
        });
    }
    group.finish();
}
criterion_group!(benches, array_mul, array_sum);
criterion_main!(benches);
