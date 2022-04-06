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
    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::{DataTypeExt, DataTypeKind};
    #[tokio::test]
    async fn test() -> Result<(), ExecutorError> {
        let left_vec = (0..100).filter(|x| x % 2 == 0).collect::<Vec<i32>>();
        let right_vec = (0..100).filter(|x| x % 4 == 0).collect::<Vec<i32>>();
        let left_child: BoxedExecutor = async_stream::try_stream! {
                yield  [
                ArrayImpl::new_int32(left_vec.into_iter().collect()),
            ]
            .into_iter()
            .collect()
        }
        .boxed();
        let right_child: BoxedExecutor = async_stream::try_stream! {
                yield  [
                ArrayImpl::new_int32(right_vec.into_iter().collect()),
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
            left_types: vec![DataTypeKind::Int(None).nullable()],
            right_types: vec![DataTypeKind::Int(None).nullable()],
        };

        let mut executor = executor.execute();

        let result_vec = (0..100).filter(|x| x % 4 == 0).collect::<Vec<i32>>();
        let chunk = executor.next().await.unwrap()?;
        assert_eq!(
            chunk.array_at(0),
            &ArrayImpl::new_int32(result_vec.clone().into_iter().collect())
        );
        assert_eq!(
            chunk.array_at(1),
            &ArrayImpl::new_int32(result_vec.into_iter().collect())
        );
        Ok(())
    }
}
