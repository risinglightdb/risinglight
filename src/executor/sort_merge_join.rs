// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.
#![allow(dead_code)]
use std::cmp::Ordering;
use std::vec::Vec;

use super::*;
use crate::array::{DataChunk, DataChunkBuilder};
use crate::types::{DataType, Row};
/// The executor for sort merge join
pub struct SortMergeJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub left_column_index: usize,
    pub right_column_index: usize,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}
impl SortMergeJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let mut left_chunks = same_key_chunks(row_stream(self.left_child), self.left_column_index);
        let mut right_chunks =
            same_key_chunks(row_stream(self.right_child), self.right_column_index);
        let mut left_current_chunk;
        let mut right_current_chunk;
        let left = left_chunks.next().await;
        let right = right_chunks.next().await;

        if let (Some(left_chunk), Some(right_chunk)) = (left, right) {
            left_current_chunk = left_chunk?;
            right_current_chunk = right_chunk?;
            // build
            let data_types = self.left_types.iter().chain(self.right_types.iter());
            let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);

            loop {
                match compare_row(
                    &left_current_chunk[0],
                    &right_current_chunk[0],
                    self.left_column_index,
                    self.right_column_index,
                ) {
                    Ordering::Equal => {
                        let full_join_chunk = full_join(&left_current_chunk, &right_current_chunk);
                        for row in full_join_chunk {
                            if let Some(chunk) = builder.push_row(row) {
                                yield chunk;
                            }
                        }

                        if let Some(chunk) = left_chunks.next().await {
                            left_current_chunk = chunk?;
                        } else {
                            break;
                        }
                    }
                    Ordering::Greater => {
                        if let Some(chunk) = right_chunks.next().await {
                            right_current_chunk = chunk?;
                        } else {
                            break;
                        }
                    }
                    Ordering::Less => {
                        if let Some(chunk) = left_chunks.next().await {
                            left_current_chunk = chunk?;
                        } else {
                            break;
                        }
                    }
                }
            }
            // if rows line < PROCESSING_WINDOW_SIZE ,take rest rows out of builder
            if let Some(chunk) = { builder }.take() {
                yield chunk;
            }
        }
    }
}
// compare two rows by join key
pub fn compare_row(
    left_row: &Row,
    right_row: &Row,
    left_column_index: usize,
    right_column_index: usize,
) -> Ordering {
    let left_data_value = &left_row[left_column_index];
    let right_data_value = &right_row[right_column_index];
    left_data_value.partial_cmp(right_data_value).unwrap()
}

// convert chunk stream to row stream
#[try_stream(boxed, ok = Row, error = ExecutorError)]
async fn row_stream(stream: BoxedExecutor) {
    #[for_await]
    for chunk in stream {
        for row in chunk?.rows() {
            yield row.values().collect();
        }
    }
}

// chunk same join key rows together
#[try_stream(boxed, ok = Vec<Row>, error = ExecutorError)]
async fn same_key_chunks(
    row_stream: BoxStream<'static, Result<Row, ExecutorError>>,
    column_index: usize,
) {
    let mut current_row = None;
    let mut chunk = Vec::new();
    #[for_await]
    for row in row_stream {
        let row = row?;
        if current_row.is_none() {
            current_row = Some(row);
            chunk.push(current_row.clone().unwrap());
            continue;
        }
        if current_row.as_ref().unwrap().get(column_index) == row.get(column_index) {
            chunk.push(row);
        } else {
            yield chunk;
            chunk = Vec::new();
            current_row = Some(row);
            chunk.push(current_row.clone().unwrap());
        }
    }
    yield chunk;
}

// for example:
// left same key rows: (join key = 0)
// 2 c
// 2 d

// right same key rows: (join key = 0)
// 2 e
// 2 f

// join_result
// 2 c 2 e
// 2 c 2 f
// 2 d 2 e
// 2 d 2 f
fn full_join(left_chunk: &Vec<Row>, right_chunk: &Vec<Row>) -> Vec<Row> {
    let mut join_chunk = Vec::new();
    for left_row in left_chunk {
        for right_row in right_chunk {
            let values = left_row
                .clone()
                .into_iter()
                .chain(right_row.clone().into_iter());
            join_chunk.push(values.collect());
        }
    }
    join_chunk
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::DataTypeKind;

    async fn sort_merge_test(left_col: Vec<i32>, right_col: Vec<i32>, expected_col: Vec<i32>) {
        let left_child: BoxedExecutor = async_stream::try_stream! {
                yield  vec![
                ArrayImpl::new_int32(left_col.into_iter().collect());2
            ]
            .into_iter()
            .collect()
        }
        .boxed();
        let right_child: BoxedExecutor = async_stream::try_stream! {
                yield  vec![
                    ArrayImpl::new_int32(right_col.into_iter().collect());2
            ]
            .into_iter()
            .collect()
        }
        .boxed();
        let executor = SortMergeJoinExecutor {
            left_child,
            right_child,
            left_column_index: 0,
            right_column_index: 0,
            left_types: vec![DataTypeKind::Int32.nullable(); 2],
            right_types: vec![DataTypeKind::Int32.nullable(); 2],
        };

        let mut executor = executor.execute();

        if let Some(chunk) = executor.next().await {
            let chunk = chunk.unwrap();
            assert_eq!(
                chunk.arrays(),
                &vec![ArrayImpl::new_int32(expected_col.clone().into_iter().collect()); 4]
            );
        } else {
            assert!(expected_col.is_empty());
        }
    }
    #[tokio::test]
    async fn test_single_element() {
        sort_merge_test(vec![1], vec![1], vec![1]).await;
        sort_merge_test(vec![1, 2, 3], vec![2, 3, 4], vec![2, 3]).await;
    }

    #[tokio::test]
    async fn test_duplicated_elements() {
        sort_merge_test(vec![1, 1], vec![1, 1], vec![1, 1, 1, 1]).await;
        sort_merge_test(
            vec![1, 2, 2, 3, 3],
            vec![2, 3, 3, 4],
            vec![2, 2, 3, 3, 3, 3],
        )
        .await;
    }

    #[tokio::test]
    async fn test_no_intersection() {
        sort_merge_test(vec![1, 2, 3], vec![4, 5, 6], vec![]).await;
    }
}
