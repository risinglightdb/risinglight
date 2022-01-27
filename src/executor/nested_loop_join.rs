// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use futures::TryStreamExt;

use super::*;
use crate::array::{Array, ArrayBuilderImpl, ArrayImpl, DataChunk};
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
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        // collect all chunks from children
        let left_chunks: Vec<DataChunk> = self.left_child.try_collect().await?;
        let right_chunks: Vec<DataChunk> = self.right_child.try_collect().await?;

        // helper functions
        let create_builders = || {
            self.left_types
                .iter()
                .chain(self.right_types.iter())
                .map(|ty| ArrayBuilderImpl::with_capacity(PROCESSING_WINDOW_SIZE, ty))
                .collect_vec()
        };
        let left_rows = || left_chunks.iter().flat_map(|chunk| chunk.rows());
        let right_rows = || right_chunks.iter().flat_map(|chunk| chunk.rows());

        // cross join: left x right
        let mut builders = create_builders();
        for left_row in left_rows() {
            for right_row in right_rows() {
                let values = left_row.values().chain(right_row.values());
                for (builder, v) in builders.iter_mut().zip_eq(values) {
                    builder.push(&v);
                }
            }
        }
        let cross_chunk = builders.into_iter().collect();

        // evaluate filter bitmap
        let filter = match self.condition.eval_array(&cross_chunk)? {
            ArrayImpl::Bool(a) => a,
            _ => panic!("unsupported value from join condition"),
        };
        yield cross_chunk.filter(filter.iter().map(|b| matches!(b, Some(true))));

        // append rows for left outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::LeftOuter | BoundJoinOperator::FullOuter
        ) {
            let mut builders = create_builders();
            let mut i = 0;
            for left_row in left_rows() {
                let mut matched = false;
                for _ in right_rows() {
                    matched |= matches!(filter.get(i), Some(true));
                    i += 1;
                }
                if matched {
                    continue;
                }
                // append row: (left, NULL)
                let values =
                    (left_row.values()).chain(self.right_types.iter().map(|_| DataValue::Null));
                for (builder, v) in builders.iter_mut().zip_eq(values) {
                    builder.push(&v);
                }
            }
            yield builders.into_iter().collect();
        }

        // append rows for right outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::RightOuter | BoundJoinOperator::FullOuter
        ) {
            let mut builders = create_builders();
            let left_row_num = left_rows().count();
            let right_row_num = right_rows().count();
            for (mut i, right_row) in right_rows().enumerate() {
                // skip if the right row matches any left rows
                let mut matched = false;
                for _ in 0..left_row_num {
                    matched |= matches!(filter.get(i), Some(true));
                    i += right_row_num;
                }
                if matched {
                    continue;
                }
                // append row: (NULL, right)
                let values =
                    (self.left_types.iter().map(|_| DataValue::Null)).chain(right_row.values());
                for (builder, v) in builders.iter_mut().zip_eq(values) {
                    builder.push(&v);
                }
            }
            yield builders.into_iter().collect();
        }
    }
}
