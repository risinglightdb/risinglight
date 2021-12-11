use std::vec::Vec;

use bitvec::bitvec;
use bitvec::vec::BitVec;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::binder::{BoundJoinConstraint, BoundJoinOperator};
use crate::types::DataValue;
// The executor for nested loop join
pub struct NestedLoopJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub join_op: BoundJoinOperator,
}

impl NestedLoopJoinExecutor {
    pub fn execute_loop_join(
        join_op: BoundJoinOperator,
        left_chunks: Vec<DataChunk>,
        right_chunks: Vec<DataChunk>,
    ) -> Result<Option<DataChunk>, ExecutorError> {
        let left_row_len = left_chunks[0].column_count();
        let right_row_len = right_chunks[0].column_count();

        let mut chunk_builders: Vec<ArrayBuilderImpl> = vec![];
        for arr in left_chunks[0].arrays() {
            chunk_builders.push(ArrayBuilderImpl::from_type_of_array(arr));
        }
        for arr in right_chunks[0].arrays() {
            chunk_builders.push(ArrayBuilderImpl::from_type_of_array(arr));
        }

        let mut left_bitmaps: Option<Vec<BitVec>> = match &join_op {
            BoundJoinOperator::LeftOuter(_) => {
                let mut vecs = vec![];
                for left_chunk in &left_chunks {
                    vecs.push(bitvec![0; left_chunk.cardinality()]);
                }
                Some(vecs)
            }
            _ => None,
        };

        let mut right_bitmaps: Option<Vec<BitVec>> = match &join_op {
            BoundJoinOperator::RightOuter(_) => {
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
                        let mut builders: Vec<ArrayBuilderImpl> = left_row
                            .iter()
                            .map(|v| ArrayBuilderImpl::new(&v.data_type().unwrap()))
                            .collect();
                        for (idx, builder) in builders.iter_mut().enumerate() {
                            builder.push(&left_row[idx]);
                        }

                        let chunk: DataChunk = builders
                            .into_iter()
                            .map(|builder| builder.finish())
                            .collect();
                        match &join_op {
                            BoundJoinOperator::Inner(constraint)
                            | BoundJoinOperator::LeftOuter(constraint)
                            | BoundJoinOperator::RightOuter(constraint) => match constraint {
                                BoundJoinConstraint::On(expr) => {
                                    let arr_impl = expr.eval_array(&chunk)?;
                                    let value = arr_impl.get(0);
                                    match value {
                                        DataValue::Bool(true) => {
                                            matched = true;
                                            match &mut right_bitmaps {
                                                Some(right_bitmaps) => {
                                                    right_bitmaps[right_chunk_idx]
                                                        .set(right_row_idx, true);
                                                }
                                                None => {}
                                            }

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

            let chunk = Self::execute_loop_join(self.join_op, left_chunks, right_chunks)?;
            if let Some(chunk) = chunk {
                yield chunk;
            }
        }
    }
}
