use super::*;
use crate::array::{Array, ArrayBuilderImpl, ArrayImpl, DataChunk};
use crate::binder::{BoundJoinConstraint, BoundJoinOperator};
use crate::types::DataValue;
use std::vec::Vec;
// The executor for nested loop join
pub struct NestedLoopJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub join_op: BoundJoinOperator,
}

impl NestedLoopJoinExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let mut left_chunks: Vec<DataChunk> = vec![];
            let mut right_chunks: Vec<DataChunk> = vec![];
            for await batch in self.left_child {
                left_chunks.push(batch?);
            }

            for await batch in self.right_child {
                right_chunks.push(batch?);
            }

            if left_chunks.len() == 0 || right_chunks.len() == 0 {
               return ;
            }
            let mut left_row = left_chunks[0].get_row_by_idx(0);
            let mut right_row = right_chunks[0].get_row_by_idx(0);
            left_row.append(&mut right_row);
            let mut chunk_builders: Vec<ArrayBuilderImpl> = left_row.iter()
                                                   .map(|v|
                                                    ArrayBuilderImpl::new_from_value(&v))
                                                    .collect();
            let mut card = 0;
            for left_chunk in left_chunks.iter() {
                for left_idx in 0..left_chunk.cardinality() {
                    for right_chunk in right_chunks.iter() {
                        for right_idx in 0..right_chunk.cardinality() {
                            let mut left_row = left_chunk.get_row_by_idx(left_idx);
                            let mut right_row = right_chunk.get_row_by_idx(right_idx);
                            left_row.append(&mut right_row);
                            let mut builders: Vec<ArrayBuilderImpl> = left_row.iter()
                                                   .map(|v|
                                                    ArrayBuilderImpl::new_from_value(&v))
                                                    .collect();
                            for (idx, builder) in builders.iter_mut().enumerate() {
                                builder.push(&left_row[idx]);
                            }
                                    
                            let arrays: Vec<ArrayImpl> = builders.into_iter().map(|builder| builder.finish()).collect();
                            let chunk = DataChunk::builder()
                            .cardinality(1)
                            .arrays(arrays.into())
                            .build();
                            let bool_val = false;
                            match &self.join_op {
                                BoundJoinOperator::Inner(constraint) => match constraint {
                                    BoundJoinConstraint::On(expr) => {
                                        let arr_impl = expr.eval_array(&chunk)?;
                                        let value = arr_impl.get_data_value_by_idx(0);
                                        match value {
                                            DataValue::Bool(val) => {
                                                if val {
                                                    for (idx, builder) in chunk_builders.iter_mut().enumerate() {
                                                        builder.push(&left_row[idx]);
                                                    }
                                                    card += 1;
                                                }
                                            }
                                            DataValue::Null => {}
                                            _ => panic!("Must be bool or null")
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            }
            let arrays: Vec<ArrayImpl> = chunk_builders.into_iter().map(|builder| builder.finish()).collect();
            yield DataChunk::builder()
            .cardinality(card)
            .arrays(arrays.into())
            .build();
           
        }
    }
}
