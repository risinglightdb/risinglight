// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};
use std::vec::Vec;

use futures::TryStreamExt;

use super::*;
use crate::array::{DataChunk, DataChunkBuilder, RowRef};
use crate::types::{DataType, DataValue};
use crate::v1::binder::{BoundExpr, BoundJoinOperator};

/// The executor for hash join
pub struct HashJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub join_op: BoundJoinOperator,
    // TODO: filter by condition
    pub condition: BoundExpr,
    pub left_column_indexes: Vec<usize>,
    pub right_column_indexes: Vec<usize>,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}

impl HashJoinExecutor {
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

        // helper functions
        let left_rows = || left_chunks.iter().flat_map(|chunk| chunk.rows());
        let right_rows = || right_chunks.iter().flat_map(|chunk| chunk.rows());

        // build
        let mut hash_map: HashMap<Vec<DataValue>, Vec<RowRef<'_>>> = HashMap::new();
        for left_row in left_rows() {
            let hash_value = left_row.get_by_indexes(&self.left_column_indexes);
            hash_map
                .entry(hash_value)
                .or_insert_with(Vec::new)
                .push(left_row);
            tokio::task::consume_budget().await;
        }

        let data_types = self.left_types.iter().chain(self.right_types.iter());
        let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);

        // probe
        for right_row in right_rows() {
            let hash_value = right_row.get_by_indexes(&self.right_column_indexes);
            for left_row in hash_map.get(&hash_value).unwrap_or(&vec![]) {
                let values = left_row.values().chain(right_row.values());
                if let Some(chunk) = builder.push_row(values) {
                    yield chunk;
                }
            }
            tokio::task::consume_budget().await;
        }

        // append rows for left outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::LeftOuter | BoundJoinOperator::FullOuter
        ) {
            let right_keys = right_rows()
                .map(|row| row.get_by_indexes(&self.right_column_indexes))
                .collect::<HashSet<Vec<DataValue>>>();
            for left_row in left_rows() {
                let hash_value = left_row.get_by_indexes(&self.left_column_indexes);
                if right_keys.contains(&hash_value) {
                    continue;
                }
                // append row: (left, NULL)
                let values =
                    (left_row.values()).chain(self.right_types.iter().map(|_| DataValue::Null));
                if let Some(chunk) = builder.push_row(values) {
                    yield chunk;
                }
                tokio::task::consume_budget().await;
            }
        }

        // append rows for right outer join
        if matches!(
            self.join_op,
            BoundJoinOperator::RightOuter | BoundJoinOperator::FullOuter
        ) {
            for right_row in right_rows() {
                let hash_value = right_row.get_by_indexes(&self.right_column_indexes);
                if hash_map.contains_key(&hash_value) {
                    continue;
                }
                // append row: (NULL, right)
                let values =
                    (self.left_types.iter().map(|_| DataValue::Null)).chain(right_row.values());
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
