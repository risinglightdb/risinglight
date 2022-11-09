// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.
#![allow(dead_code)]
use smallvec::SmallVec;

use super::*;
use crate::array::ArrayImpl::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl, *};
use crate::types::{DataTypeKind, DataValue};
use crate::v1::binder::{BoundAggCall, BoundExpr};
use crate::v1::executor::aggregation::AggregationState;

/// The executor of perfect hash aggregation.
/// Used by low range group keys
pub struct PerfectHashAggExecutor {
    agg_calls: Vec<BoundAggCall>,
    group_keys: Vec<BoundExpr>,
    child: BoxedExecutor,
    bits: Vec<usize>,
    min_values: Vec<DataValue>,
    total_bits_num: usize,
    groups_num: usize,
}

impl PerfectHashAggExecutor {
    pub fn new(
        group_keys: Vec<BoundExpr>,
        agg_calls: Vec<BoundAggCall>,
        child: BoxedExecutor,
        bits: Vec<usize>,
        min_values: Vec<DataValue>,
    ) -> Self {
        let bits_num = bits.iter().sum();
        let groups_num = 1usize << bits_num;

        // Avoid allocating too much memory
        // TODO: This should be decided by the planner
        assert!(bits_num < 12);

        Self {
            agg_calls,
            group_keys,
            child,
            bits,
            min_values,
            total_bits_num: bits_num,
            groups_num,
        }
    }
}

type StateVec = Vec<Option<SmallVec<[Box<dyn AggregationState>; 16]>>>;

impl PerfectHashAggExecutor {
    fn execute_inner(
        group_keys: &[BoundExpr],
        agg_calls: &[BoundAggCall],
        bits: &[usize],
        min_value: &[DataValue],
        total_bits_num: usize,
        states: &mut StateVec,
        chunk: DataChunk,
    ) -> Result<(), ExecutorError> {
        let group_cols: SmallVec<[ArrayImpl; 16]> =
            group_keys.iter().map(|e| e.eval(&chunk)).try_collect()?;
        let locations = Self::compute_location(bits, min_value, total_bits_num, &group_cols);
        locations.iter().for_each(|group| {
            if states[*group].is_none() {
                states[*group] = Some(create_agg_states(agg_calls));
            }
        });

        let arrays: SmallVec<[ArrayImpl; 16]> = agg_calls
            .iter()
            .map(|agg| agg.args[0].eval(&chunk))
            .try_collect()?;

        // Update states

        let num_rows = chunk.cardinality();

        let col_cnt = arrays.len();
        for col_idx in 0..col_cnt {
            let array = &arrays[col_idx];
            for row_idx in 0..num_rows {
                let state = &mut states[locations[row_idx]].as_mut().unwrap()[col_idx];
                // TODO: Update Batch?
                state.update_single(&array.get(row_idx))?
            }
        }

        Ok(())
    }

    fn build_chunk(
        group_keys: &[BoundExpr],
        agg_calls: &[BoundAggCall],
        bits: &[usize],
        min_values: &[DataValue],
        total_bits_num: usize,
        states: &StateVec,
        locations: &[usize],
    ) -> DataChunk {
        let mut key_builders = group_keys
            .iter()
            .map(|e| ArrayBuilderImpl::new(&e.return_type()))
            .collect::<Vec<ArrayBuilderImpl>>();
        let mut res_builders = agg_calls
            .iter()
            .map(|agg| ArrayBuilderImpl::new(&agg.return_type))
            .collect::<Vec<ArrayBuilderImpl>>();
        let mut need_shift_bits_num = total_bits_num;
        (0..group_keys.len()).for_each(|idx| {
            need_shift_bits_num -= bits[idx];
            let mask = (1usize << bits[idx]) - 1;
            let key_builder = &mut key_builders[idx];
            match group_keys[idx].return_type().kind {
                DataTypeKind::Bool => locations.iter().for_each(|location| {
                    let value = (location >> need_shift_bits_num) & mask;
                    if value == 0 {
                        key_builder.push(&DataValue::Null);
                    } else if value == 1 {
                        key_builder.push(&DataValue::Bool(false));
                    } else if value == 2 {
                        key_builder.push(&DataValue::Bool(true));
                    } else {
                        unreachable!();
                    }
                }),
                DataTypeKind::Int32 => {
                    let min = if let DataValue::Int32(x) = min_values[idx] {
                        x
                    } else {
                        unreachable!();
                    };
                    locations.iter().for_each(|location| {
                        let value = ((location >> need_shift_bits_num) & mask) as i32;
                        if value == 0 {
                            key_builder.push(&DataValue::Null);
                        } else {
                            key_builder.push(&DataValue::Int32(value + min - 1));
                        }
                    })
                }
                DataTypeKind::Int64 => {
                    let min = if let DataValue::Int64(x) = min_values[idx] {
                        x
                    } else {
                        unreachable!();
                    };
                    locations.iter().for_each(|location| {
                        let value = ((location >> need_shift_bits_num) & mask) as i64;
                        if value == 0 {
                            key_builder.push(&DataValue::Null);
                        } else {
                            key_builder.push(&DataValue::Int64(value + min - 1));
                        }
                    })
                }
                _ => unreachable!(),
            }
        });

        (0..agg_calls.len()).for_each(|idx| {
            locations.iter().for_each(|location| {
                let states = states[*location].as_ref().unwrap();
                // batch?
                res_builders[idx].push(&states[idx].output());
            });
        });

        key_builders.append(&mut res_builders);
        key_builders.into_iter().collect()
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    async fn finish_agg(
        group_keys: Vec<BoundExpr>,
        agg_calls: Vec<BoundAggCall>,
        bits: Vec<usize>,
        min_value: Vec<DataValue>,
        total_bits_num: usize,
        states: StateVec,
    ) {
        let mut locations = Vec::with_capacity(PROCESSING_WINDOW_SIZE);
        let mut count = 0usize;

        for (idx, state) in states.iter().enumerate() {
            if state.is_some() {
                locations.push(idx);
                count += 1;
                if count == PROCESSING_WINDOW_SIZE {
                    yield Self::build_chunk(
                        &group_keys,
                        &agg_calls,
                        &bits,
                        &min_value,
                        total_bits_num,
                        &states,
                        &locations,
                    );
                    count = 0;
                    locations.clear();
                }
            }
        }

        if !locations.is_empty() {
            yield Self::build_chunk(
                &group_keys,
                &agg_calls,
                &bits,
                &min_value,
                total_bits_num,
                &states,
                &locations,
            );
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let mut states = Vec::with_capacity(self.groups_num);
        (0..self.groups_num)
            .into_iter()
            .for_each(|_| states.push(None));
        #[for_await]
        for chunk in self.child {
            let chunk = chunk?;
            Self::execute_inner(
                &self.group_keys,
                &self.agg_calls,
                &self.bits,
                &self.min_values,
                self.total_bits_num,
                &mut states,
                chunk,
            )?;
        }

        #[for_await]
        for chunk in Self::finish_agg(
            self.group_keys,
            self.agg_calls,
            self.bits,
            self.min_values,
            self.total_bits_num,
            states,
        ) {
            let chunk = chunk?;
            yield chunk
        }
    }

    fn compute_location(
        bits: &[usize],
        min_values: &[DataValue],
        total_bits_num: usize,
        chunk: &SmallVec<[ArrayImpl; 16]>,
    ) -> Vec<usize> {
        let mut locations = Vec::new();
        let count = chunk.first().unwrap().len();
        locations.resize(count, 0);
        let mut need_shift_bits_num = total_bits_num;
        chunk.iter().enumerate().for_each(|(idx, array)| {
            need_shift_bits_num -= bits[idx];
            match array {
                Bool(inner) => {
                    // let valid_mask = inner.get_valid_bitmap();
                    inner.iter().enumerate().for_each(|(i, x)| {
                        if let Some(x) = x {
                            if *x {
                                locations[i] += 2usize << need_shift_bits_num;
                            } else {
                                locations[i] += 1usize << need_shift_bits_num;
                            }
                        }
                    });
                }
                Int32(inner) => {
                    let min;
                    if let DataValue::Int32(x) = min_values[idx] {
                        min = x;
                    } else {
                        unreachable!();
                    }
                    inner.iter().enumerate().for_each(|(i, x)| {
                        if let Some(x) = x {
                            locations[i] += ((*x - min + 1) as usize) << need_shift_bits_num;
                        }
                    });
                }
                Int64(inner) => {
                    let min;
                    if let DataValue::Int64(x) = min_values[idx] {
                        min = x;
                    } else {
                        unreachable!();
                    }
                    inner.iter().enumerate().for_each(|(i, x)| {
                        if let Some(x) = x {
                            locations[i] += ((*x - min + 1) as usize) << need_shift_bits_num;
                        }
                    });
                }
                _ => unreachable!(),
            }
        });
        locations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::{DataType, DataTypeKind};
    use crate::v1::binder::{AggKind, BoundInputRef};

    #[tokio::test]
    async fn perfect_hash_aggregate_test() {
        let col_a = vec![3, 7, 8, 7];
        let col_b = vec![80, 100, 102, 100];
        let expected_group_cols_a = vec![3, 7, 8];
        let expected_group_cols_b = vec![80, 100, 102];
        let expected_function_res = vec![80, 200, 102];

        let child: BoxedExecutor = async_stream::try_stream! {
                yield  vec![
                ArrayImpl::new_int32(col_a.into_iter().collect()),
                ArrayImpl::new_int32(col_b.into_iter().collect())
            ]
            .into_iter()
            .collect()
        }
        .boxed();

        let group_keys = vec![
            BoundExpr::InputRef(BoundInputRef {
                index: 0,
                return_type: DataType::new(DataTypeKind::Int32, true),
            }),
            BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: DataType::new(DataTypeKind::Int32, true),
            }),
        ];

        let sum_function = vec![BoundAggCall {
            kind: AggKind::Sum,
            args: vec![group_keys[1].clone()],
            return_type: DataType::new(DataTypeKind::Int32, true),
        }];

        // col_a: [min = 3, max = 8, range = 6 + 1(null) -> 0b111, bit_num = 3]
        // col_b: [min = 80, max = 102, ...]
        let executor = PerfectHashAggExecutor::new(
            group_keys,
            sum_function,
            child,
            vec![3, 5],
            vec![DataValue::Int32(3), DataValue::Int32(80)],
        );

        let mut executor = executor.execute();

        if let Some(chunk) = executor.next().await {
            let chunk = chunk.unwrap();
            assert_eq!(
                chunk.arrays()[0],
                ArrayImpl::new_int32(expected_group_cols_a.clone().into_iter().collect())
            );
            assert_eq!(
                chunk.arrays()[1],
                ArrayImpl::new_int32(expected_group_cols_b.clone().into_iter().collect())
            );
            assert_eq!(
                chunk.arrays()[2],
                ArrayImpl::new_int32(expected_function_res.clone().into_iter().collect())
            );
        }
    }
}
