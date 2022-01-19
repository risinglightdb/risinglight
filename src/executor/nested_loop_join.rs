// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use bitvec::bitvec;
use bitvec::vec::BitVec;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::binder::{BoundExpr, BoundJoinOperator};
use crate::types::{DataType, DataValue};

/// The executor for nested loop join.
pub struct NestedLoopJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub join_op: BoundJoinOperator,
    pub condition: BoundExpr,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}

impl NestedLoopJoinExecutor {
    fn execute_loop_join(
        join_op: BoundJoinOperator,
        join_cond: BoundExpr,
        left_chunks: Vec<DataChunk>,
        right_chunks: Vec<DataChunk>,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
    ) -> Result<Option<DataChunk>, ExecutorError> {
        let left_row_len = left_types.len();
        let right_row_len = left_types.len();

        let mut chunk_builders: Vec<ArrayBuilderImpl> = vec![];
        for ty in &left_types {
            chunk_builders.push(ArrayBuilderImpl::new(ty));
        }
        for ty in &right_types {
            chunk_builders.push(ArrayBuilderImpl::new(ty));
        }

        let mut left_bitmaps: Option<Vec<BitVec>> = match &join_op {
            BoundJoinOperator::LeftOuter | BoundJoinOperator::FullOuter => {
                let mut vecs = vec![];
                for left_chunk in &left_chunks {
                    vecs.push(bitvec![0; left_chunk.cardinality()]);
                }
                Some(vecs)
            }
            _ => None,
        };

        let mut right_bitmaps: Option<Vec<BitVec>> = match &join_op {
            BoundJoinOperator::RightOuter | BoundJoinOperator::FullOuter => {
                let mut vecs = vec![];
                for right_chunk in &right_chunks {
                    vecs.push(bitvec![0; right_chunk.cardinality()]);
                }
                Some(vecs)
            }
            _ => None,
        };

        for left_chunk_idx in 0..left_chunks.len() {
            for left_row_idx in 0..left_chunks[left_chunk_idx].cardinality() {
                let mut matched = false;
                for right_chunk_idx in 0..right_chunks.len() {
                    for right_row_idx in 0..right_chunks[right_chunk_idx].cardinality() {
                        let mut left_row = left_chunks[left_chunk_idx].get_row_by_idx(left_row_idx);
                        let mut right_row =
                            right_chunks[right_chunk_idx].get_row_by_idx(right_row_idx);
                        left_row.append(&mut right_row);
                        let mut builders = vec![];
                        for ty in &left_types {
                            builders.push(ArrayBuilderImpl::new(ty));
                        }
                        for ty in &right_types {
                            builders.push(ArrayBuilderImpl::new(ty));
                        }
                        for (idx, builder) in builders.iter_mut().enumerate() {
                            builder.push(&left_row[idx]);
                        }

                        let chunk: DataChunk = builders
                            .into_iter()
                            .map(|builder| builder.finish())
                            .collect();
                        let arr_impl = join_cond.eval_array(&chunk)?;
                        let value = arr_impl.get(0);
                        match value {
                            DataValue::Bool(true) => {
                                matched = true;
                                match &mut right_bitmaps {
                                    Some(right_bitmaps) => {
                                        right_bitmaps[right_chunk_idx].set(right_row_idx, true);
                                    }
                                    None => {}
                                }

                                for (idx, builder) in chunk_builders.iter_mut().enumerate() {
                                    builder.push(&left_row[idx]);
                                }
                            }
                            DataValue::Bool(false) => {}
                            _ => {
                                panic!("unsupported value from join condition")
                            }
                        }
                    }
                }
                match &mut left_bitmaps {
                    Some(left_bitmaps) => {
                        if matched {
                            left_bitmaps[left_chunk_idx].set(left_row_idx, true);
                        }
                    }
                    None => {}
                }
            }
        }
        match &left_bitmaps {
            Some(left_bitmaps) => {
                for left_chunk_idx in 0..left_chunks.len() {
                    for left_row_idx in 0..left_chunks[left_chunk_idx].cardinality() {
                        if !left_bitmaps[left_chunk_idx][left_row_idx] {
                            let mut left_row =
                                left_chunks[left_chunk_idx].get_row_by_idx(left_row_idx);
                            for _ in 0..right_row_len {
                                left_row.push(DataValue::Null);
                            }

                            for (idx, builder) in chunk_builders.iter_mut().enumerate() {
                                builder.push(&left_row[idx]);
                            }
                        }
                    }
                }
            }
            None => {}
        }

        match &right_bitmaps {
            Some(right_bitmaps) => {
                for right_chunk_idx in 0..right_chunks.len() {
                    for right_row_idx in 0..right_chunks[right_chunk_idx].cardinality() {
                        if !right_bitmaps[right_chunk_idx][right_row_idx] {
                            let mut row = vec![];
                            let mut righ_row =
                                right_chunks[right_chunk_idx].get_row_by_idx(right_row_idx);
                            for _ in 0..left_row_len {
                                row.push(DataValue::Null);
                            }
                            row.append(&mut righ_row);
                            for (idx, builder) in chunk_builders.iter_mut().enumerate() {
                                builder.push(&row[idx]);
                            }
                        }
                    }
                }
            }
            None => {}
        }
        Ok(Some(
            chunk_builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect(),
        ))
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let mut left_chunks: Vec<DataChunk> = vec![];
        let mut right_chunks: Vec<DataChunk> = vec![];
        #[for_await]
        for batch in self.left_child {
            left_chunks.push(batch?);
        }

        #[for_await]
        for batch in self.right_child {
            right_chunks.push(batch?);
        }

        let chunk = Self::execute_loop_join(
            self.join_op,
            self.condition,
            left_chunks,
            right_chunks,
            self.left_types,
            self.right_types,
        )?;
        if let Some(chunk) = chunk {
            yield chunk;
        }
    }
}
