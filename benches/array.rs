// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use bitvec::vec::BitVec;
use criterion::*;
use ordered_float::OrderedFloat;
use risinglight::array::{
    ArrayFromDataExt, ArrayImpl, BoolArray, DecimalArray, F64Array, I32Array,
};
use risinglight::parser::BinaryOperator;
use risinglight::types::DataType;
use rust_decimal::Decimal;

fn ops(c: &mut Criterion) {
    for ty in ["i32", "f64", "decimal"] {
        for op in ["add", "mul", "div", "eq", "gt"] {
            if op == "div" && ty != "f64" {
                // FIXME: handle panic: division by 0
                continue;
            }
            for_all_size(c, format!("{op}({ty},{ty})"), |b, &size| {
                let a1 = match ty {
                    "i32" => make_i32_array(size),
                    "f64" => make_f64_array(size),
                    "decimal" => make_decimal_array(size),
                    _ => unreachable!(),
                };
                let op = match op {
                    "add" => BinaryOperator::Plus,
                    "mul" => BinaryOperator::Multiply,
                    "div" => BinaryOperator::Divide,
                    "eq" => BinaryOperator::Eq,
                    "gt" => BinaryOperator::Gt,
                    _ => unreachable!(),
                };
                b.iter(|| a1.binary_op(&op, &a1));
            });
        }
    }

    for_all_size(c, "and(bool,bool)", |b, &size| {
        let a1: ArrayImpl = make_bool_array(size);
        let a2: ArrayImpl = make_bool_array(size);
        b.iter(|| a1.and(&a2));
    });
    for_all_size(c, "or(bool,bool)", |b, &size| {
        let a1: ArrayImpl = make_bool_array(size);
        let a2: ArrayImpl = make_bool_array(size);
        b.iter(|| a1.or(&a2));
    });
    for_all_size(c, "not(bool)", |b, &size| {
        let a1: ArrayImpl = make_bool_array(size);
        b.iter(|| a1.not());
    });
}

fn agg(c: &mut Criterion) {
    for ty in ["i32", "f64", "decimal"] {
        let make_array = |size| match ty {
            "i32" => make_i32_array(size),
            "f64" => make_f64_array(size),
            "decimal" => make_decimal_array(size),
            _ => unreachable!(),
        };
        for_all_size(c, format!("sum({ty})"), |b, &size| {
            let a1 = make_array(size);
            b.iter(|| a1.sum())
        });
        for_all_size(c, format!("max({ty})"), |b, &size| {
            let a1 = make_array(size);
            b.iter(|| a1.max_())
        });
        for_all_size(c, format!("first({ty})"), |b, &size| {
            let a1 = make_array(size);
            b.iter(|| a1.first())
        });
        for_all_size(c, format!("count({ty})"), |b, &size| {
            let a1 = make_array(size);
            b.iter(|| a1.count())
        });
    }
}

fn cast(c: &mut Criterion) {
    for_all_size(c, "cast(i32->f64)", |b, &size| {
        let a1 = make_i32_array(size);
        b.iter(|| a1.cast(&DataType::Float64))
    });
    for_all_size(c, "cast(f64->decimal)", |b, &size| {
        let a1 = make_f64_array(size);
        b.iter(|| a1.cast(&DataType::Decimal(None, None)))
    });
    for ty in ["i32", "f64", "decimal"] {
        for_all_size(c, format!("cast({ty}->string)"), |b, &size| {
            let a1 = match ty {
                "i32" => make_i32_array(size),
                "f64" => make_f64_array(size),
                "decimal" => make_decimal_array(size),
                _ => unreachable!(),
            };
            b.iter(|| a1.cast(&DataType::String))
        });
    }
}

fn filter(c: &mut Criterion) {
    for_all_size(c, "filter(i32)", |b, &size| {
        let a1 = make_i32_array(size);
        let ArrayImpl::Bool(a2) = make_bool_array(size) else {
            unreachable!()
        };
        b.iter(|| a1.filter(a2.true_array()))
    });
}

fn make_bool_array(size: usize) -> ArrayImpl {
    let mask = make_valid_bitmap(size);
    let iter = (0..size as i32)
        .zip(mask.clone())
        .map(|(i, v)| if v { i % 2 == 0 } else { false });
    BoolArray::from_data(iter, mask).into()
}

fn make_i32_array(size: usize) -> ArrayImpl {
    let mask = make_valid_bitmap(size);
    let iter = (0..size as i32)
        .zip(mask.clone())
        .map(|(i, v)| if v { i } else { 0 });
    I32Array::from_data(iter, mask).into()
}

fn make_f64_array(size: usize) -> ArrayImpl {
    let mask = make_valid_bitmap(size);
    let iter = (0..size)
        .zip(mask.clone())
        .map(|(i, v)| OrderedFloat(if v { i as f64 } else { 0.0 }));
    F64Array::from_data(iter, mask).into()
}

fn make_decimal_array(size: usize) -> ArrayImpl {
    let mask = make_valid_bitmap(size);
    let iter = (0..size)
        .zip(mask.clone())
        .map(|(i, v)| Decimal::from(if v { i } else { 0 }));
    DecimalArray::from_data(iter, mask).into()
}

fn make_valid_bitmap(size: usize) -> BitVec {
    (0..size)
        .map(|i| {
            let i = i % 192;
            if i < 128 {
                true
            } else {
                i % 2 == 0
            }
        })
        .collect()
}

fn for_all_size(
    c: &mut Criterion,
    name: impl Into<String>,
    mut f: impl FnMut(&mut Bencher<'_, measurement::WallTime>, &usize),
) {
    let mut group = c.benchmark_group(name);
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in [1, 16, 256, 4096] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, &mut f);
    }
    group.finish();
}

criterion_group!(benches, ops, agg, cast, filter);
criterion_main!(benches);
