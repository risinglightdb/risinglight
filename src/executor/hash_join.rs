// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::vec::Vec;

use futures::TryStreamExt;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk, RowRef};
use crate::binder::{BoundExpr, BoundJoinOperator};
use crate::types::{DataType, DataValue};

/// The executor for hash join
pub struct HashJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub join_op: BoundJoinOperator,
    pub condition: BoundExpr,
    pub left_column_index: usize,
    pub right_column_index: usize,
    pub data_types: Vec<DataType>,
}

// TODO : support other types of join: left/right/full join
impl HashJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        // collect all chunks from children
        let left_chunks: Vec<DataChunk> = self.left_child.try_collect().await?;
        let right_chunks: Vec<DataChunk> = self.right_child.try_collect().await?;

        // helper functions
        let left_rows = || left_chunks.iter().flat_map(|chunk| chunk.rows());
        let right_rows = || right_chunks.iter().flat_map(|chunk| chunk.rows());

        // build
        let mut hash_map: HashMap<DataValue, Vec<RowRef<'_>>> = HashMap::new();
        for left_row in left_rows() {
            let hash_value = left_row.get(self.left_column_index);
            hash_map
                .entry(hash_value)
                .or_insert_with(Vec::new)
                .push(left_row);
        }

        // probe
        let mut builders = self
            .data_types
            .iter()
            .map(|ty| ArrayBuilderImpl::with_capacity(PROCESSING_WINDOW_SIZE, ty))
            .collect_vec();
        for right_row in right_rows() {
            let hash_value = right_row.get(self.right_column_index);
            for left_row in hash_map.get(&hash_value).unwrap_or(&vec![]) {
                let values = left_row.values().chain(right_row.values());
                for (builder, v) in builders.iter_mut().zip(values) {
                    builder.push(&v);
                }
            }
        }
        yield builders.into_iter().collect();
    }
}
