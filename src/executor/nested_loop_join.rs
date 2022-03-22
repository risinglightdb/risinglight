// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use futures::TryStreamExt;

use super::*;
use crate::array::{Array, ArrayBuilderImpl, ArrayImpl, DataChunk, DataChunkBuilder};
use crate::binder::{BoundExpr, BoundJoinOperator};
use crate::types::{DataType, DataValue, PhysicalDataTypeKind};

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
        let (left_chunks, right_chunks) = async {
            tokio::try_join!(
                self.left_child.try_collect::<Vec<DataChunk>>(),
                self.right_child.try_collect::<Vec<DataChunk>>(),
            )
        }
        .await?;

        let left_rows = || left_chunks.iter().flat_map(|chunk| chunk.rows());
        let right_rows = || right_chunks.iter().flat_map(|chunk| chunk.rows());

        let data_types = self.left_types.iter().chain(self.right_types.iter());
        let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);
        let mut filter_builder = ArrayBuilderImpl::with_capacity_and_physical(
            PROCESSING_WINDOW_SIZE,
            PhysicalDataTypeKind::Bool,
        );
        // cross join: left x right
        for left_row in left_rows() {
            for right_row in right_rows() {
                let values = left_row.values().chain(right_row.values());
                if let Some(chunk) = builder.push_row(values) {
                    // evaluate filter bitmap
                    match self.condition.eval(&chunk)? {
                        ArrayImpl::Bool(a) => {
                            yield chunk.filter(a.iter().map(|b| matches!(b, Some(true))));
                            filter_builder.append(&ArrayImpl::Bool(a))
                        }
                        _ => panic!("unsupported value from join condition"),
                    }
                }
            }
        }
        // take rest of data
        if let Some(chunk) = builder.take() {
            match self.condition.eval(&chunk)? {
                ArrayImpl::Bool(a) => {
                    yield chunk.filter(a.iter().map(|b| matches!(b, Some(true))));
                    filter_builder.append(&ArrayImpl::Bool(a))
                }
                _ => panic!("unsupported value from join condition"),
            }
        }

        let filter = match filter_builder.take() {
            ArrayImpl::Bool(a) => a,
            _ => panic!("unsupported value from join condition"),
        };

        // append rows for left outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::LeftOuter | BoundJoinOperator::FullOuter
        ) {
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
                if let Some(chunk) = builder.push_row(values) {
                    yield chunk;
                }
            }
        }

        // append rows for right outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::RightOuter | BoundJoinOperator::FullOuter
        ) {
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
                if let Some(chunk) = builder.push_row(values) {
                    yield chunk;
                }
            }
        }

        if let Some(chunk) = { builder }.take() {
            yield chunk;
        }
    }
}
