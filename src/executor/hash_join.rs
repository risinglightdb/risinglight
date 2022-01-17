use std::collections::HashMap;
use std::vec::Vec;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
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
    fn execute_hash_join(
        left_chunks: Vec<DataChunk>,
        right_chunks: Vec<DataChunk>,
        left_column_index: usize,
        right_column_index: usize,
        data_types: Vec<DataType>,
    ) -> Result<Option<DataChunk>, ExecutorError> {
        // Build hash table

        let mut hash_map: HashMap<DataValue, Vec<Vec<DataValue>>> = HashMap::new();
        for left_chunk in &left_chunks {
            for left_row_idx in 0..left_chunk.cardinality() {
                let row = left_chunk.get_row_by_idx(left_row_idx);
                let hash_data_value = &row[left_column_index];

                hash_map
                    .entry(hash_data_value.clone())
                    .or_insert_with(Vec::new);
                let val = hash_map.get_mut(hash_data_value).unwrap();
                val.push(row);
            }
        }
        let mut chunk_builders: Vec<ArrayBuilderImpl> = vec![];
        for ty in &data_types {
            chunk_builders.push(ArrayBuilderImpl::new(ty));
        }
        // Probe
        for right_chunk in &right_chunks {
            for right_row_idx in 0..right_chunk.cardinality() {
                let right_row = right_chunk.get_row_by_idx(right_row_idx);
                let hash_data_value = &right_row[right_column_index];

                if !hash_map.contains_key(hash_data_value) {
                    continue;
                }
                let rows = hash_map.get(hash_data_value).unwrap();
                for left_row in rows.iter() {
                    if left_row[left_column_index] == right_row[right_column_index] {
                        for (idx, builder) in chunk_builders.iter_mut().enumerate() {
                            if idx < left_row.len() {
                                builder.push(&left_row[idx]);
                            } else {
                                builder.push(&right_row[idx - left_row.len()]);
                            }
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

        let chunk = Self::execute_hash_join(
            left_chunks,
            right_chunks,
            self.left_column_index,
            self.right_column_index,
            self.data_types,
        )?;
        if let Some(chunk) = chunk {
            yield chunk;
        }
    }
}
