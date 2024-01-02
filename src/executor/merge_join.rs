// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use super::*;
use crate::array::{DataChunk, DataChunkBuilder};
use crate::types::{DataType, DataValue, Row};

/// The executor for merge join.
pub struct MergeJoinExecutor<const T: JoinType> {
    pub left_keys: RecExpr,
    pub right_keys: RecExpr,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}

impl<const T: JoinType> MergeJoinExecutor<T> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, left: BoxedExecutor, right: BoxedExecutor) {
        let mut left_groups = group_by_keys(left, self.left_keys);
        let mut right_groups = group_by_keys(right, self.right_keys);
        let mut left_group = left_groups.next().await.transpose()?;
        let mut right_group = right_groups.next().await.transpose()?;

        let data_types = self.left_types.iter().chain(self.right_types.iter());
        let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);

        loop {
            match (&left_group, &right_group) {
                // cross join if left key == right key
                (Some((lkey, lchunk)), Some((rkey, rchunk))) if lkey == rkey => {
                    for left_row in lchunk {
                        for right_row in rchunk {
                            let values = left_row.iter().chain(right_row.iter()).cloned();
                            if let Some(chunk) = builder.push_row(values) {
                                yield chunk;
                            }
                        }
                    }
                    left_group = left_groups.next().await.transpose()?;
                    right_group = right_groups.next().await.transpose()?;
                }
                // left join if left key < right key or right is finished
                (Some((lkey, lchunk)), _)
                    if right_group.as_ref().map_or(true, |(rkey, _)| lkey < rkey) =>
                {
                    if T == JoinType::LeftOuter || T == JoinType::FullOuter {
                        for left_row in lchunk {
                            let values = (left_row.iter().cloned())
                                .chain(self.right_types.iter().map(|_| DataValue::Null));
                            if let Some(chunk) = builder.push_row(values) {
                                yield chunk;
                            }
                        }
                    }
                    left_group = left_groups.next().await.transpose()?;
                }
                // right join if left key > right key or left is finished
                (_, Some((rkey, rchunk)))
                    if left_group.as_ref().map_or(true, |(lkey, _)| lkey > rkey) =>
                {
                    if T == JoinType::RightOuter || T == JoinType::FullOuter {
                        for right_row in rchunk {
                            let values = (self.left_types.iter().map(|_| DataValue::Null))
                                .chain(right_row.iter().cloned());
                            if let Some(chunk) = builder.push_row(values) {
                                yield chunk;
                            }
                        }
                    }
                    right_group = right_groups.next().await.transpose()?;
                }
                _ => break,
            }
        }
        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

/// Group rows by keys.
///
/// This function takes a stream of chunks and outputs a stream of groups.
#[try_stream(boxed, ok = (Row, Vec<Row>), error = ExecutorError)]
async fn group_by_keys(stream: BoxedExecutor, keys_expr: RecExpr) {
    let mut current_key = Vec::new();
    let mut output_rows = Vec::new();

    #[for_await]
    for input in stream {
        let input = input?;
        let keys = Evaluator::new(&keys_expr).eval_list(&input)?;
        for (row, keys) in input.rows().zip(keys.rows()) {
            if keys != &current_key {
                let output_keys = std::mem::replace(&mut current_key, keys.to_owned());
                let output_rows = std::mem::take(&mut output_rows);
                if !output_keys.is_empty() {
                    yield (output_keys, output_rows);
                }
            }
            output_rows.push(row.to_owned());
        }
    }
    if !current_key.is_empty() {
        yield (current_key, output_rows);
    }
}
