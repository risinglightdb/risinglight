// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::ConstParamTy;
use std::vec::Vec;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use smallvec::SmallVec;

use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl, DataChunk, DataChunkBuilder, RowRef};
use crate::types::{DataType, DataValue, Row};

/// The executor for hash join
pub struct HashJoinExecutor<const T: JoinType> {
    pub left_keys: RecExpr,
    pub right_keys: RecExpr,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}

/// Join types for generating join code during the compilation.
#[derive(Copy, Clone, Eq, PartialEq, ConstParamTy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

pub type JoinKeys = SmallVec<[DataValue; 2]>;

impl<const T: JoinType> HashJoinExecutor<T> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, left: BoxedExecutor, right: BoxedExecutor) {
        // build
        #[derive(Default, Debug)]
        struct LeftKeyInfo {
            rows: SmallVec<[Row; 1]>,
            matched: bool,
        }
        let mut hash_map: HashMap<JoinKeys, LeftKeyInfo> = HashMap::new();
        #[for_await]
        for chunk in left {
            let chunk = chunk?;
            let keys_chunk = Evaluator::new(&self.left_keys).eval_list(&chunk)?;
            for (row, keys) in chunk.rows().zip(keys_chunk.rows()) {
                let keys = keys.values().collect();
                hash_map.entry(keys).or_default().rows.push(row.to_owned());
            }
            tokio::task::consume_budget().await;
        }

        let data_types = self.left_types.iter().chain(self.right_types.iter());
        let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);

        // probe
        #[for_await]
        for chunk in right {
            let chunk = chunk?;
            let keys_chunk = Evaluator::new(&self.right_keys).eval_list(&chunk)?;
            for (right_row, keys) in chunk.rows().zip(keys_chunk.rows()) {
                if let Some(left_rows) = hash_map.get_mut(&keys.values().collect::<JoinKeys>()) {
                    left_rows.matched = true;
                    for left_row in &left_rows.rows {
                        let values = left_row.iter().cloned().chain(right_row.values());
                        if let Some(chunk) = builder.push_row(values) {
                            yield chunk;
                        }
                    }
                } else if T == JoinType::RightOuter || T == JoinType::FullOuter {
                    // append row: (NULL, right)
                    let values =
                        (self.left_types.iter().map(|_| DataValue::Null)).chain(right_row.values());
                    if let Some(chunk) = builder.push_row(values) {
                        yield chunk;
                    }
                }
            }
            tokio::task::consume_budget().await;
        }

        // append rows for left outer join
        if T == JoinType::LeftOuter || T == JoinType::FullOuter {
            for (_, rows) in hash_map {
                if rows.matched {
                    continue;
                }
                for row in rows.rows {
                    // append row: (left, NULL)
                    let values =
                        (row.into_iter()).chain(self.right_types.iter().map(|_| DataValue::Null));
                    if let Some(chunk) = builder.push_row(values) {
                        yield chunk;
                    }
                }
                tokio::task::consume_budget().await;
            }
        }

        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

/// The executor for hash semi/anti join
pub struct HashSemiJoinExecutor {
    pub left_keys: RecExpr,
    pub right_keys: RecExpr,
    pub anti: bool,
}

impl HashSemiJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, left: BoxedExecutor, right: BoxedExecutor) {
        let mut key_set: HashSet<JoinKeys> = HashSet::new();
        // build
        #[for_await]
        for chunk in right {
            let chunk = chunk?;
            let keys_chunk = Evaluator::new(&self.right_keys).eval_list(&chunk)?;
            for row in keys_chunk.rows() {
                key_set.insert(row.values().collect());
            }
            tokio::task::consume_budget().await;
        }
        // probe
        #[for_await]
        for chunk in left {
            let chunk = chunk?;
            let keys_chunk = Evaluator::new(&self.left_keys).eval_list(&chunk)?;
            let exists = keys_chunk
                .rows()
                .map(|key| key_set.contains(&key.values().collect::<JoinKeys>()) ^ self.anti)
                .collect::<Vec<bool>>();
            yield chunk.filter(&exists);
        }
    }
}

/// The executor for hash semi/anti join
pub struct HashSemiJoinExecutor2 {
    pub left_keys: RecExpr,
    pub right_keys: RecExpr,
    pub condition: RecExpr,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
    pub anti: bool,
}

impl HashSemiJoinExecutor2 {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, left: BoxedExecutor, right: BoxedExecutor) {
        let mut key_set: HashMap<JoinKeys, DataChunkBuilder> = HashMap::new();
        // build
        #[for_await]
        for chunk in right {
            let chunk = chunk?;
            let keys_chunk = Evaluator::new(&self.right_keys).eval_list(&chunk)?;
            for (key, row) in keys_chunk.rows().zip(chunk.rows()) {
                let chunk = key_set
                    .entry(key.values().collect())
                    .or_insert_with(|| DataChunkBuilder::unbounded(&self.right_types))
                    .push_row(row.values());
                assert!(chunk.is_none());
            }
            tokio::task::consume_budget().await;
        }
        let key_set = key_set
            .into_iter()
            .map(|(k, mut v)| (k, v.take().unwrap()))
            .collect::<HashMap<JoinKeys, DataChunk>>();
        // probe
        #[for_await]
        for chunk in left {
            let chunk = chunk?;
            let keys_chunk = Evaluator::new(&self.left_keys).eval_list(&chunk)?;
            let mut exists = Vec::with_capacity(chunk.cardinality());
            for (key, lrow) in keys_chunk.rows().zip(chunk.rows()) {
                let b = if let Some(rchunk) = key_set.get(&key.values().collect::<JoinKeys>()) {
                    let lchunk = self.left_row_to_chunk(&lrow, rchunk.cardinality());
                    let join_chunk = lchunk.row_concat(rchunk.clone());
                    let ArrayImpl::Bool(a) = Evaluator::new(&self.condition).eval(&join_chunk)?
                    else {
                        panic!("join condition should return bool");
                    };
                    a.true_array().iter().any(|b| *b)
                } else {
                    false
                };
                exists.push(b ^ self.anti);
            }
            yield chunk.filter(&exists);
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
