// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use futures::TryStreamExt;

use super::*;
use crate::array::{Array, ArrayBuilderImpl, ArrayImpl, DataChunk, DataChunkBuilder};
use crate::types::{DataType, DataTypeKind, DataValue};
use crate::v1::binder::{BoundExpr, BoundJoinOperator};

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
        // only support inner and left outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::RightOuter | BoundJoinOperator::FullOuter
        ) {
            panic!("unsupported join type");
        }
        let left_chunks = self.left_child.try_collect::<Vec<DataChunk>>().await?;

        let left_rows = || left_chunks.iter().flat_map(|chunk| chunk.rows());

        let data_types = self.left_types.iter().chain(self.right_types.iter());
        let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);
        let mut filter_builder =
            ArrayBuilderImpl::with_capacity(PROCESSING_WINDOW_SIZE, &DataTypeKind::Bool.not_null());

        let mut right_row_num = 0;
        // cross join: left x right
        #[for_await]
        for right_chunk in self.right_child {
            let right_chunk = right_chunk?;
            let right_rows = right_chunk.rows();
            for right_row in right_rows {
                for left_row in left_rows() {
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
                    tokio::task::consume_budget().await;
                }
            }
            right_row_num += right_chunk.cardinality();
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
        if matches!(self.join_op, BoundJoinOperator::LeftOuter) {
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

        if let Some(chunk) = { builder }.take() {
            yield chunk;
        }
    }
}
