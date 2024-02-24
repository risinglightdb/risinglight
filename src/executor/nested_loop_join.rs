// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use futures::TryStreamExt;

use super::*;
use crate::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, BoolArrayBuilder, DataChunk,
    DataChunkBuilder, RowRef,
};
use crate::types::{DataType, DataValue};

/// The executor for nested loop join.
pub struct NestedLoopJoinExecutor {
    pub op: Expr,
    pub condition: RecExpr,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}

impl NestedLoopJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, left_child: BoxedExecutor, right_child: BoxedExecutor) {
        if !matches!(self.op, Expr::Inner | Expr::LeftOuter) {
            todo!("unsupported join type: {:?}", self.op);
        }
        let left_chunks = left_child.try_collect::<Vec<DataChunk>>().await?;

        let left_rows = || left_chunks.iter().flat_map(|chunk| chunk.rows());

        let data_types = self.left_types.iter().chain(self.right_types.iter());
        let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);
        let mut filter_builder = BoolArrayBuilder::with_capacity(PROCESSING_WINDOW_SIZE);

        let mut right_row_num = 0;
        // inner join: left x right
        #[for_await]
        for right_chunk in right_child {
            let right_chunk = right_chunk?;
            for right_row in right_chunk.rows() {
                for left_row in left_rows() {
                    let values = left_row.values().chain(right_row.values());
                    if let Some(chunk) = builder.push_row(values) {
                        // evaluate filter bitmap
                        let ArrayImpl::Bool(a) = Evaluator::new(&self.condition).eval(&chunk)?
                        else {
                            panic!("join condition should return bool");
                        };
                        yield chunk.filter(a.true_array());
                        filter_builder.append(&a);
                    }
                    tokio::task::consume_budget().await;
                }
            }
            right_row_num += right_chunk.cardinality();
        }

        // take rest of data
        if let Some(chunk) = builder.take() {
            // evaluate filter bitmap
            let ArrayImpl::Bool(a) = Evaluator::new(&self.condition).eval(&chunk)? else {
                panic!("join condition should return bool");
            };
            yield chunk.filter(a.true_array());
            filter_builder.append(&a);
        }
        let filter = filter_builder.take();

        // append rows for left outer join
        if matches!(self.op, Expr::LeftOuter) {
            // we need to pick row of left_row which unmatched rows
            let left_row_num = left_rows().count();
            for (mut i, left_row) in left_rows().enumerate() {
                let mut matched = false;
                for _ in 0..right_row_num {
                    // the `filter` has all matching results of the cross join result `right x left`
                    // to compute the unmatched rows, we will need to first pick a row of left_row
                    // (namely the i-th row). then we check if all `filter[i + left_row_num * j]`
                    matched |= matches!(filter.get(i), Some(true));
                    i += left_row_num;
                }
                if matched {
                    continue;
                }
                // if all false, we append row: (left, NULL)
                let values =
                    (left_row.values()).chain(self.right_types.iter().map(|_| DataValue::Null));
                if let Some(chunk) = builder.push_row(values) {
                    yield chunk;
                }
                tokio::task::consume_budget().await;
            }
        }

        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

/// The executor for nested loop semi/anti join.
pub struct NestedLoopSemiJoinExecutor {
    pub anti: bool,
    pub condition: RecExpr,
    pub left_types: Vec<DataType>,
}

impl NestedLoopSemiJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, left_child: BoxedExecutor, right_child: BoxedExecutor) {
        let right_chunks = right_child.try_collect::<Vec<DataChunk>>().await?;

        let mut builder = DataChunkBuilder::new(&self.left_types, PROCESSING_WINDOW_SIZE);

        #[for_await]
        for left_chunk in left_child {
            let left_chunk = left_chunk?;
            'left_row: for left_row in left_chunk.rows() {
                let mut exists = false;
                for right_chunk in &right_chunks {
                    let left_chunk = self.left_row_to_chunk(&left_row, right_chunk.cardinality());
                    let join_chunk = left_chunk.row_concat(right_chunk.clone());
                    // evaluate filter bitmap
                    let ArrayImpl::Bool(a) = Evaluator::new(&self.condition).eval(&join_chunk)?
                    else {
                        panic!("join condition should return bool");
                    };
                    exists |= a.true_array().iter().any(|v| *v);
                    if exists && !self.anti {
                        if let Some(chunk) = builder.push_row(left_row.values()) {
                            yield chunk;
                        }
                        continue 'left_row;
                    }
                    tokio::task::consume_budget().await;
                }
                if exists ^ self.anti {
                    if let Some(chunk) = builder.push_row(left_row.values()) {
                        yield chunk;
                    }
                }
            }
        }
        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }

    /// Expand the left row to a chunk with given length.
    fn left_row_to_chunk(&self, row: &RowRef<'_>, len: usize) -> DataChunk {
        self.left_types
            .iter()
            .zip(row.values())
            .map(|(ty, value)| {
                let mut builder = ArrayBuilderImpl::with_capacity(len, ty);
                builder.push_n(len, &value);
                builder.finish()
            })
            .collect()
    }
}
