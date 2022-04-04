// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::vec::Vec;

use super::*;
use crate::array::{DataChunk, RowRef};
use crate::types::DataType;

/// The executor for sort merge join
pub struct SortMergeJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub left_column_indexes: Vec<usize>,
    pub right_column_indexes: Vec<usize>,
    pub left_types: Vec<DataType>,
    pub right_types: Vec<DataType>,
}
impl SortMergeJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let left_rows = self.left_child.flat_map(move |chunk| row_stream(chunk));
        let right_rows = self.right_child.flat_map(move |chunk| row_stream(chunk));
        #[for_await]
        for row in left_rows {
            let row = row.unwrap().values().next().unwrap();
            println!("{:?}", row);
        }
        // // collect all chunks from children
        // let (left_chunks, right_chunks) = async {
        //     tokio::try_join!(
        //         self.left_child.try_collect::<Vec<DataChunk>>(),
        //         self.right_child.try_collect::<Vec<DataChunk>>(),
        //     )
        // }
        // .await?;

        // // get rows iterator from chunks
        // let left_rows = || left_chunks.iter().flat_map(|chunk| chunk.rows()).peekable();
        // let right_rows = || {
        //     right_chunks
        //         .iter()
        //         .flat_map(|chunk| chunk.rows())
        //         .peekable()
        // };

        // // build
        // let data_types = self.left_types.iter().chain(self.right_types.iter());
        // let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);
        // let mut left_row = left_rows().next().unwrap();
        // let mut right_row = right_rows().next().unwrap();
        // loop {
        //     match compare_row(
        //         &left_row,
        //         &right_row,
        //         &self.left_column_indexes,
        //         &self.right_column_indexes,
        //     ) {
        //         Ordering::Equal => {
        //             let values = left_row.values().chain(right_row.values());
        //             if let Some(chunk) = builder.push_row(values) {
        //                 yield chunk;
        //             }
        //             if left_rows().peek().is_none() {
        //                 break;
        //             }
        //             left_row = left_rows().next().unwrap();
        //         }
        //         Ordering::Greater => {
        //             if right_rows().peek().is_none() {
        //                 break;
        //             }
        //             right_row = right_rows().next().unwrap();
        //         }
        //         Ordering::Less => {
        //             if left_rows().peek().is_none() {
        //                 break;
        //             }
        //             left_row = left_rows().next().unwrap();
        //         }
        //     }
        // }
        // // if rows line < PROCESSING_WINDOW_SIZE ,take rest rows out of builder
        // if let Some(chunk) = { builder }.take() {
        //     yield chunk;
        // }
    }
}

pub fn compare_row(
    left_row: &RowRef,
    right_row: &RowRef,
    left_column_indexes: &[usize],
    right_column_indexes: &[usize],
) -> Ordering {
    let left_data_value = left_row.get_by_indexes(left_column_indexes)[0].clone();
    let right_data_value = right_row.get_by_indexes(right_column_indexes)[0].clone();
    left_data_value.partial_cmp(&right_data_value).unwrap()
}

#[try_stream(boxed,ok = RowRef<'static>, error = ExecutorError)]
async fn row_stream(result: Result<DataChunk, ExecutorError>) {
    match result {
        Ok(chunk) => {
            for row in chunk.rows() {
                yield row
            }
        }
        Err(err) => yield Err(err)?,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::{DataTypeExt, DataTypeKind};
    #[tokio::test]
    async fn test() {
        let left_child: BoxedExecutor = async_stream::try_stream! {
                yield  [
                ArrayImpl::new_int32([1,2,3,4].into_iter().collect()),
            ]
            .into_iter()
            .collect()
        }
        .boxed();
        let right_child: BoxedExecutor = async_stream::try_stream! {
                yield  [
                ArrayImpl::new_int32([2,3,4,5].into_iter().collect()),
            ]
            .into_iter()
            .collect()
        }
        .boxed();
        let executor = SortMergeJoinExecutor {
            left_child,
            right_child,
            left_column_indexes: vec![0],
            right_column_indexes: vec![0],
            left_types: vec![DataTypeKind::Int(None).nullable()],
            right_types: vec![DataTypeKind::Int(None).nullable()],
        };

        let mut executor = executor.execute();
        for chunk in executor.next().await.unwrap() {
            println!("{:?}", chunk);
        }
    }
}
