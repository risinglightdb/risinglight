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
        let mut left_rows = row_stream(self.left_child);
        let mut right_rows = row_stream(self.right_child);
        let mut left_row;
        let mut right_row;
        let first_left_row = left_rows.next().await;
        let first_right_row = right_rows.next().await;
        if first_left_row.is_none() || first_right_row.is_none() {
            yield Err(ExecutorError::NotNullable)?
        } else {
            left_row = first_left_row.unwrap()?;
            right_row = first_right_row.unwrap()?;
            // build
            let data_types = self.left_types.iter().chain(self.right_types.iter());
            let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);

            loop {
                match compare_row(
                    &left_row,
                    &right_row,
                    self.left_column_index,
                    self.right_column_index,
                ) {
                    Ordering::Equal => {
                        let values = left_row.into_iter().chain(right_row.clone().into_iter());
                        if let Some(chunk) = builder.push_row(values) {
                            yield chunk;
                        }
                        if let Some(row) = left_rows.next().await {
                            left_row = row?;
                        } else {
                            break;
                        }
                    }
                    Ordering::Greater => {
                        if let Some(row) = right_rows.next().await {
                            right_row = row?;
                        } else {
                            break;
                        }
                    }
                    Ordering::Less => {
                        if let Some(row) = left_rows.next().await {
                            left_row = row?;
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

pub fn compare_row(
    left_row: &Row,
    right_row: &Row,
    left_column_indexes: usize,
    right_column_indexes: usize,
) -> Ordering {
    let left_data_value = &left_row[left_column_indexes];
    let right_data_value = &right_row[right_column_indexes];
    left_data_value.partial_cmp(right_data_value).unwrap()
}

#[try_stream(boxed, ok = Row, error = ExecutorError)]
async fn row_stream(stream: BoxedExecutor) {
    #[for_await]
    for chunk in stream {
        for row in chunk?.rows() {
            yield row.values().collect();
        }
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::{DataTypeExt, DataTypeKind};
    #[test_case(vec![1],vec![1],vec![1])]
    #[test_case(vec![1],vec![1,2,3],vec![1])]
    #[test_case(vec![1,2,3,4],vec![2,4,6],vec![2,4])]
    #[tokio::test]
    async fn sort_merge_test(left_col: Vec<i32>, right_col: Vec<i32>, expected_col: Vec<i32>) {
        let left_child: BoxedExecutor = async_stream::try_stream! {
                yield  [
                ArrayImpl::new_int32(left_col.clone().into_iter().collect()),
                ArrayImpl::new_int32(left_col.into_iter().collect())
            ]
            .into_iter()
            .collect()
        }
        .boxed();
        let right_child: BoxedExecutor = async_stream::try_stream! {
                yield  [
                    ArrayImpl::new_int32(right_col.clone().into_iter().collect()),
                    ArrayImpl::new_int32(right_col.into_iter().collect())
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
            left_types: vec![DataTypeKind::Int(None).nullable(); 2],
            right_types: vec![DataTypeKind::Int(None).nullable(); 2],
        };

        let mut executor = executor.execute();

        let chunk = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.arrays(),
            &vec![ArrayImpl::new_int32(expected_col.clone().into_iter().collect()); 4]
        );
    }
}
