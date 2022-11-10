// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};
use std::vec::Vec;

use futures::TryStreamExt;
use smallvec::SmallVec;

use super::*;
use crate::array::{DataChunk, DataChunkBuilder, RowRef};
use crate::types::{DataType, DataValue};

/// The executor for hash join
pub struct HashJoinExecutor {
    pub op: Expr,
    pub left_keys: RecExpr,
    pub right_keys: RecExpr,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}

pub type JoinKeys = SmallVec<[DataValue; 2]>;

impl HashJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, left: BoxedExecutor, right: BoxedExecutor) {
        // collect all chunks from children
        let (left_chunks, right_chunks) = async {
            tokio::try_join!(
                left.try_collect::<Vec<DataChunk>>(),
                right.try_collect::<Vec<DataChunk>>(),
            )
        }
        .await?;

        // build
        let mut hash_map: HashMap<JoinKeys, SmallVec<[RowRef<'_>; 1]>> = HashMap::new();
        for chunk in &left_chunks {
            let keys_chunk = Evaluator::new(&self.left_keys).eval_list(chunk)?;
            for i in 0..chunk.cardinality() {
                let keys = keys_chunk.row(i).values().collect();
                let row = chunk.row(i);
                hash_map.entry(keys).or_insert_with(SmallVec::new).push(row);
                tokio::task::consume_budget().await;
            }
        }

        let data_types = self.left_types.iter().chain(self.right_types.iter());
        let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);
        let mut right_keys = HashSet::new();

        // probe
        for chunk in &right_chunks {
            let keys_chunk = Evaluator::new(&self.right_keys).eval_list(chunk)?;
            for i in 0..chunk.cardinality() {
                let right_row = chunk.row(i);
                let keys: JoinKeys = keys_chunk.row(i).values().collect();
                if matches!(self.op, Expr::LeftOuter | Expr::FullOuter) {
                    right_keys.insert(keys.clone());
                }
                if let Some(left_rows) = hash_map.get(&keys) {
                    for left_row in left_rows {
                        let values = left_row.values().chain(right_row.values());
                        if let Some(chunk) = builder.push_row(values) {
                            yield chunk;
                        }
                    }
                } else if matches!(self.op, Expr::RightOuter | Expr::FullOuter) {
                    // append row: (NULL, right)
                    let values =
                        (self.left_types.iter().map(|_| DataValue::Null)).chain(right_row.values());
                    if let Some(chunk) = builder.push_row(values) {
                        yield chunk;
                    }
                }
                tokio::task::consume_budget().await;
            }
        }

        // append rows for left outer join
        if matches!(self.op, Expr::LeftOuter | Expr::FullOuter) {
            for chunk in &left_chunks {
                let keys_chunk = Evaluator::new(&self.left_keys).eval_list(chunk)?;
                for i in 0..chunk.cardinality() {
                    let keys: JoinKeys = keys_chunk.row(i).values().collect();
                    let row = chunk.row(i);
                    if right_keys.contains(&keys) {
                        continue;
                    }
                    // append row: (left, NULL)
                    let values =
                        (row.values()).chain(self.right_types.iter().map(|_| DataValue::Null));
                    if let Some(chunk) = builder.push_row(values) {
                        yield chunk;
                    }
                    tokio::task::consume_budget().await;
                }
            }
        }

        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}
