// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use criterion::*;
use risinglight::array::{ArrayFromDataExt, ArrayImpl, BoolArray, I32Array};
use risinglight::v1::function::FunctionCtx;

#[inline(never)]
fn never_inline_mul(a: &i32, b: &i32, _: &mut FunctionCtx) -> i32 {
    *a * *b
}

fn make_i32_array(size: usize) -> ArrayImpl {
    let mask = (0..size)
        .map(|i| {
            let i = i % 192;
            if i < 128 {
                true
            } else {
                i % 2 == 0
            }
        })
        .collect();

    I32Array::from_data(0..size as i32, mask).into()
}

fn array_mul(c: &mut Criterion) {
    let mut group = c.benchmark_group("add(i32,i32)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let a1 = make_i32_array(size);
            let a2 = make_i32_array(size);
            b.iter(|| a1.add(&a2));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("mul(i32,i32)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let a1 = make_i32_array(size);
            let a2 = make_i32_array(size);
            b.iter(|| a1.mul(&a2));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("and(bool,bool)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let a1: ArrayImpl = (0..size).map(|i| i % 2 == 0).collect::<BoolArray>().into();
            let a2: ArrayImpl = a1.clone();
            b.iter(|| a1.and(&a2));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("array mul function (standard)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            use risinglight::v1::function::BinaryExecutor;
            let a1 = make_i32_array(size);
            let a2 = make_i32_array(size);
            let f = |x: &i32, y: &i32, _: &mut FunctionCtx| (*x) * (*y);
            b.iter(|| {
                let _ = BinaryExecutor::eval_batch_standard::<I32Array, I32Array, I32Array, _>(
                    &a1, &a2, f,
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
            let a1 = make_i32_array(size);
            let a2 = make_i32_array(size);
            b.iter(|| {
                let _ = BinaryExecutor::eval_batch_standard::<I32Array, I32Array, I32Array, _>(
                    &a1,
                    &a2,
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
            let a1 = make_i32_array(size);
            let a2 = make_i32_array(size);
            let f = |x: &i32, y: &i32, _: &mut FunctionCtx| (*x) * (*y);
            b.iter(|| {
                let _ = BinaryExecutor::eval_batch_lazy_select::<I32Array, I32Array, I32Array, _>(
                    &a1, &a2, f,
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
            let a1 = make_i32_array(size);
            let a2 = make_i32_array(size);
            b.iter(|| {
                let _ = BinaryExecutor::eval_batch_lazy_select::<I32Array, I32Array, I32Array, _>(
                    &a1,
                    &a2,
                    never_inline_mul,
                );
            });
        });
    }
    group.finish();
}

fn array_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("array sum");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096, 65536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let a1 = ArrayImpl::new_int32((0..size).collect());
            b.iter(|| a1.sum())
        });
    }
    group.finish();
}

criterion_group!(benches, array_mul, array_sum);
criterion_main!(benches);
