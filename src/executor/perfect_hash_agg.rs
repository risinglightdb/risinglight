// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use smallvec::SmallVec;

use super::*;
use crate::array::ArrayImpl::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl, *};
use crate::binder::{BoundAggCall, BoundExpr};
use crate::executor::aggregation::AggregationState;
use crate::types::{DataValue, PhysicalDataTypeKind};

/// The executor of hash aggregation.
pub struct PerfectHashAggExecutor {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: BoxedExecutor,
    pub bits: Vec<usize>,
    pub min_values: Vec<DataValue>,
    pub total_bits_num: usize,
    pub groups_num: usize,
    // pub states: Vec<Option<SmallVec<[Box<dyn AggregationState>; 16]>>>,
    // pub marker: Vec<bool>,
}

impl PerfectHashAggExecutor {
    pub fn new(
        agg_calls: Vec<BoundAggCall>,
        group_keys: Vec<BoundExpr>,
        child: BoxedExecutor,
        bits: Vec<usize>,
        min_values: Vec<DataValue>,
    ) -> Self {
        let bits_num = bits.iter().sum();
        let groups_num = 1usize << bits_num;
        Self {
            agg_calls,
            group_keys,
            child,
            bits,
            min_values,
            total_bits_num: bits_num,
            groups_num,
            // states,
            // marker,
        }
    }
}

impl PerfectHashAggExecutor {
    fn execute_inner(
        group_keys: &Vec<BoundExpr>,
        agg_calls: &Vec<BoundAggCall>,
        bits: &Vec<usize>,
        min_value: &Vec<DataValue>,
        total_bits_num: usize,
        states: &mut Vec<Option<SmallVec<[Box<dyn AggregationState>; 16]>>>,
        chunk: DataChunk,
    ) -> Result<(), ExecutorError> {
        let group_cols: SmallVec<[ArrayImpl; 16]> =
            group_keys.iter().map(|e| e.eval(&chunk)).try_collect()?;
        let locations = Self::compute_location(bits, min_value, total_bits_num, &group_cols);
        locations.iter().for_each(|group| {
            if states[*group].is_none() {
                states[*group] = Some(create_agg_states(&agg_calls));
            }
        });

        let arrays: SmallVec<[ArrayImpl; 16]> = agg_calls
            .iter()
            .map(|agg| agg.args[0].eval(&chunk))
            .try_collect()?;

        // Update states
        let num_rows = chunk.cardinality();
        for row_idx in 0..num_rows {
            let states = states[locations[row_idx]].as_mut().unwrap();
            for (array, state) in arrays.iter().zip_eq(states.iter_mut()) {
                // TODO: support aggregations with multiple arguments
                state.update_single(&array.get(row_idx))?;
            }
        }

        Ok(())
    }

    fn build_chunk(
        group_keys: &Vec<BoundExpr>,
        agg_calls: &Vec<BoundAggCall>,
        bits: &Vec<usize>,
        min_values: &Vec<DataValue>,
        total_bits_num: usize,
        states: &Vec<Option<SmallVec<[Box<dyn AggregationState>; 16]>>>,
        locations: &Vec<usize>,
    ) -> DataChunk {
        let mut key_builders = group_keys
            .iter()
            .map(|e| ArrayBuilderImpl::new(&e.return_type().unwrap()))
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
            match group_keys[idx].return_type().unwrap().physical_kind() {
                PhysicalDataTypeKind::Bool => locations.iter().for_each(|location| {
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
                PhysicalDataTypeKind::Int32 => locations.iter().for_each(|location| {
                    let value = ((location >> need_shift_bits_num) & mask) as i32;
                    if value == 0 {
                        key_builder.push(&DataValue::Null);
                    }
                    if let DataValue::Int32(min) = min_values[idx] {
                        key_builder.push(&DataValue::Int32(value + min - 1));
                    } else {
                        unreachable!();
                    }
                }),
                PhysicalDataTypeKind::Int64 => locations.iter().for_each(|location| {
                    let value = ((location >> need_shift_bits_num) & mask) as i64;
                    if value == 0 {
                        key_builder.push(&DataValue::Null);
                    }
                    if let DataValue::Int64(min) = min_values[idx] {
                        key_builder.push(&DataValue::Int64(value + min - 1));
                    } else {
                        unreachable!();
                    }
                }),
                _ => unreachable!(),
            }
        });

        (0..agg_calls.len()).for_each(|idx| {
            locations.iter().for_each(|location| {
                let states = states[*location].as_ref().unwrap();
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
        states:  Vec<Option<SmallVec<[Box<dyn AggregationState>; 16]>>>,
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
        (0..self.groups_num).into_iter().for_each(|_| states.push(None));
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
        bits: &Vec<usize>,
        min_values: &Vec<DataValue>,
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
                    inner.iter().for_each(|x| match x {
                        Some(x) => {
                            if *x {
                                locations[idx] = 2usize << need_shift_bits_num;
                            } else {
                                locations[idx] = 1usize << need_shift_bits_num;
                            }
                        }
                        None => {}
                    });
                }
                Int32(inner) => {
                    let min;
                    if let DataValue::Int32(x) = min_values[idx] {
                        min = x;
                    } else {
                        unreachable!();
                    }
                    inner.iter().for_each(|x| match x {
                        Some(x) => {
                            locations[idx] = ((*x - min + 1) as usize) << need_shift_bits_num;
                        }
                        None => {}
                    });
                }
                Int64(inner) => {
                    let min;
                    if let DataValue::Int64(x) = min_values[idx] {
                        min = x;
                    } else {
                        unreachable!();
                    }
                    inner.iter().for_each(|x| match x {
                        Some(x) => {
                            locations[idx] = ((*x - min + 1) as usize) << need_shift_bits_num;
                        }
                        None => {}
                    });
                }
                _ => unreachable!(),
            }
        });
        locations
    }
}
