// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};
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
    // TODO: filter by condition
    pub condition: BoundExpr,
    pub left_column_index: usize,
    pub right_column_index: usize,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}

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
        let mut builders = (self.left_types.iter())
            .chain(self.right_types.iter())
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

        // append rows for left outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::LeftOuter | BoundJoinOperator::FullOuter
        ) {
            let right_keys = right_rows()
                .map(|row| row.get(self.right_column_index))
                .collect::<HashSet<DataValue>>();
            for left_row in left_rows() {
                let hash_value = left_row.get(self.left_column_index);
                if right_keys.contains(&hash_value) {
                    continue;
                }
                // append row: (left, NULL)
                let values =
                    (left_row.values()).chain(self.right_types.iter().map(|_| DataValue::Null));
                for (builder, v) in builders.iter_mut().zip(values) {
                    builder.push(&v);
                }
            }
        }

        // append rows for right outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::RightOuter | BoundJoinOperator::FullOuter
        ) {
            for right_row in right_rows() {
                let hash_value = right_row.get(self.right_column_index);
                if hash_map.contains_key(&hash_value) {
                    continue;
                }
                // append row: (NULL, right)
                let values =
                    (self.left_types.iter().map(|_| DataValue::Null)).chain(right_row.values());
                for (builder, v) in builders.iter_mut().zip(values) {
                    builder.push(&v);
                }
            }
        }

        yield builders.into_iter().collect();
    }
}
