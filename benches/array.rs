// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bitvec::vec::BitVec;
use criterion::*;
use risinglight::array::{ArrayFromDataExt, ArrayImpl, I32Array};
use risinglight::v1::function::FunctionCtx;

#[inline(never)]
fn never_inline_mul(a: &i32, b: &i32, _: &mut FunctionCtx) -> i32 {
    *a * *b
}

fn array_mul(c: &mut Criterion) {
    let mut group = c.benchmark_group("array mul binary op");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::array::ops;

            let mut mask_a = BitVec::new();
            let mut mask_b = BitVec::new();
            let mut i = 0;
            (0..size).into_iter().for_each(|_| {
                if i == 192 {
                    i = 0;
                }
                if i < 128 {
                    mask_a.push(true);
                    mask_b.push(true);
                } else if (128..192).contains(&i) {
                    mask_a.push(i % 2 == 0);
                    mask_b.push(i % 2 == 0);
                } else {
                    unreachable!();
                }
                i += 1;
            });

            let a1 = I32Array::from_data(0..size, mask_a);
            let a2 = I32Array::from_data(0..size, mask_b);

            b.iter(|| {
                let _: I32Array = ops::binary_op(&a1, &a2, |a, b| a * b);
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("array mul function (standard)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::v1::function::BinaryExecutor;
            let mut mask_a = BitVec::new();
            let mut mask_b = BitVec::new();

            let mut i = 0;
            (0..size).into_iter().for_each(|_| {
                if i == 192 {
                    i = 0;
                }
                if i < 128 {
                    mask_a.push(true);
                    mask_b.push(true);
                } else if (128..192).contains(&i) {
                    mask_a.push(i % 2 == 0);
                    mask_b.push(i % 2 == 0);
                } else {
                    unreachable!();
                }
                i += 1;
            });

            let array_a: ArrayImpl = I32Array::from_data(0..size, mask_a).into();
            let array_b: ArrayImpl = I32Array::from_data(0..size, mask_b).into();
            let f = |x: &i32, y: &i32, _: &mut FunctionCtx| (*x) * (*y);
            b.iter(|| {
                let _ = BinaryExecutor::eval_batch_standard::<I32Array, I32Array, I32Array, _>(
                    &array_a, &array_b, f,
                );
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("array mul function (standard + never inline )");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::v1::function::BinaryExecutor;
            let mut mask_a = BitVec::new();
            let mut mask_b = BitVec::new();

            let mut i = 0;
            (0..size).into_iter().for_each(|_| {
                if i == 192 {
                    i = 0;
                }
                if i < 128 {
                    mask_a.push(true);
                    mask_b.push(true);
                } else if (128..192).contains(&i) {
                    mask_a.push(i % 2 == 0);
                    mask_b.push(i % 2 == 0);
                } else {
                    unreachable!();
                }
                i += 1;
            });

            let array_a: ArrayImpl = I32Array::from_data(0..size, mask_a).into();
            let array_b: ArrayImpl = I32Array::from_data(0..size, mask_b).into();
            b.iter(|| {
                let _ = BinaryExecutor::eval_batch_standard::<I32Array, I32Array, I32Array, _>(
                    &array_a,
                    &array_b,
                    never_inline_mul,
                );
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("array mul function (lazy select)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::v1::function::BinaryExecutor;
            let mut mask_a = BitVec::new();
            let mut mask_b = BitVec::new();

            let mut i = 0;
            (0..size).into_iter().for_each(|_| {
                if i == 192 {
                    i = 0;
                }
                if i < 128 {
                    mask_a.push(true);
                    mask_b.push(true);
                } else if (128..192).contains(&i) {
                    mask_a.push(i % 2 == 0);
                    mask_b.push(i % 2 == 0);
                } else {
                    unreachable!();
                }
                i += 1;
            });

            let array_a: ArrayImpl = I32Array::from_data(0..size, mask_a).into();
            let array_b: ArrayImpl = I32Array::from_data(0..size, mask_b).into();
            let f = |x: &i32, y: &i32, _: &mut FunctionCtx| (*x) * (*y);
            b.iter(|| {
                let _ = BinaryExecutor::eval_batch_lazy_select::<I32Array, I32Array, I32Array, _>(
                    &array_a, &array_b, f,
                );
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("array mul function (lazy select + never inline)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::v1::function::BinaryExecutor;
            let mut mask_a = BitVec::new();
            let mut mask_b = BitVec::new();

            let mut i = 0;
            (0..size).into_iter().for_each(|_| {
                if i == 192 {
                    i = 0;
                }
                if i < 128 {
                    mask_a.push(true);
                    mask_b.push(true);
                } else if (128..192).contains(&i) {
                    mask_a.push(i % 2 == 0);
                    mask_b.push(i % 2 == 0);
                } else {
                    unreachable!();
                }
                i += 1;
            });

            let array_a: ArrayImpl = I32Array::from_data(0..size, mask_a).into();
            let array_b: ArrayImpl = I32Array::from_data(0..size, mask_b).into();
            b.iter(|| {
                let _ = BinaryExecutor::eval_batch_lazy_select::<I32Array, I32Array, I32Array, _>(
                    &array_a,
                    &array_b,
                    never_inline_mul,
                );
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("array mul simd");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::array::ops;
            let mut mask_a = BitVec::new();
            let mut mask_b = BitVec::new();
            let mut i = 0;
            (0..size).into_iter().for_each(|_| {
                if i == 192 {
                    i = 0;
                }
                if i < 128 {
                    mask_a.push(true);
                    mask_b.push(true);
                } else if (128..192).contains(&i) {
                    mask_a.push(i % 2 == 0);
                    mask_b.push(i % 2 == 0);
                } else {
                    unreachable!();
                }
                i += 1;
            });

            let a1 = I32Array::from_data(0..size, mask_a);
            let a2 = I32Array::from_data(0..size, mask_b);
            b.iter(|| {
                let _: I32Array = ops::simd_op::<_, _, _, 32>(&a1, &a2, |a, b| a * b);
            });
        });
    }
    group.finish();
}

fn array_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("array sum");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536, 1048576] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::array::Array;
            use risinglight::v1::executor::sum_i32;
            let a1: I32Array = (0..size).collect();
            b.iter(|| {
                a1.iter().fold(None, sum_i32);
            })
        });
    }
    group.finish();

    let mut group = c.benchmark_group("array sum simd");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536, 1048576] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let a1: I32Array = (0..size).collect();
            b.iter(|| {
                a1.batch_iter::<32>().sum::<i32>();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, array_mul, array_sum);
criterion_main!(benches);
