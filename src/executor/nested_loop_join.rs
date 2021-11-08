use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
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
    pub fn execute_inner(
        join_op: BoundJoinOperator,
        left_chunks: Vec<DataChunk>,
        right_chunks: Vec<DataChunk>,
    ) -> Result<Option<DataChunk>, ExecutorError> {
        if left_chunks.is_empty() || right_chunks.is_empty() {
            return Ok(None);
        }

        let mut left_row = left_chunks[0].get_row_by_idx(0);
        let mut right_row = right_chunks[0].get_row_by_idx(0);
        left_row.append(&mut right_row);
        let mut chunk_builders: Vec<ArrayBuilderImpl> = left_row
            .iter()
            .map(ArrayBuilderImpl::from_type_of_value)
            .collect();

        for left_chunk in left_chunks.iter() {
            for left_idx in 0..left_chunk.cardinality() {
                for right_chunk in right_chunks.iter() {
                    for right_idx in 0..right_chunk.cardinality() {
                        let mut left_row = left_chunk.get_row_by_idx(left_idx);
                        let mut right_row = right_chunk.get_row_by_idx(right_idx);
                        left_row.append(&mut right_row);
                        let mut builders: Vec<ArrayBuilderImpl> = left_row
                            .iter()
                            .map(ArrayBuilderImpl::from_type_of_value)
                            .collect();
                        for (idx, builder) in builders.iter_mut().enumerate() {
                            builder.push(&left_row[idx]);
                        }

                        let chunk: DataChunk = builders
                            .into_iter()
                            .map(|builder| builder.finish())
                            .collect();
                        match &join_op {
                            BoundJoinOperator::Inner(constraint) => match constraint {
                                BoundJoinConstraint::On(expr) => {
                                    let arr_impl = expr.eval_array(&chunk)?;
                                    let value = arr_impl.get(0);
                                    match value {
                                        DataValue::Bool(true) => {
                                            for (idx, builder) in
                                                chunk_builders.iter_mut().enumerate()
                                            {
                                                builder.push(&left_row[idx]);
                                            }
                                        }
                                        DataValue::Bool(false) => {}
                                        _ => {
                                            panic!("unsupported value from join condition")
                                        }
                                    }
                                }
                            },
                        }
                    }
                }
            }
        }

        Ok(Some(
            chunk_builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect(),
        ))
    }

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

            let chunk = Self::execute_inner(self.join_op, left_chunks, right_chunks)?;
            if let Some(chunk) = chunk {
                yield chunk;
            }
        }
    }
}
