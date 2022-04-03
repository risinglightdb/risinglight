// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::vec::Vec;

use futures::TryStreamExt;

use super::*;
use crate::array::{DataChunk, DataChunkBuilder, RowRef};
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
#[allow(dead_code)]
impl SortMergeJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    #[allow(dead_code)]
    pub async fn execute(self) {
        // collect all chunks from children
        let (left_chunks, right_chunks) = async {
            tokio::try_join!(
                self.left_child.try_collect::<Vec<DataChunk>>(),
                self.right_child.try_collect::<Vec<DataChunk>>(),
            )
        }
        .await?;

        // get rows iterator from chunks
        let left_rows = || left_chunks.iter().flat_map(|chunk| chunk.rows()).peekable();
        let right_rows = || {
            right_chunks
                .iter()
                .flat_map(|chunk| chunk.rows())
                .peekable()
        };

        // build
        let data_types = self.left_types.iter().chain(self.right_types.iter());
        let mut builder = DataChunkBuilder::new(data_types, PROCESSING_WINDOW_SIZE);
        let mut left_row = left_rows().next().unwrap();
        let mut right_row = right_rows().next().unwrap();
        loop {
            match compare_row(
                &left_row,
                &right_row,
                &self.left_column_indexes,
                &self.right_column_indexes,
            ) {
                Ordering::Equal => {
                    let values = left_row.values().chain(right_row.values());
                    if let Some(chunk) = builder.push_row(values) {
                        yield chunk;
                    }
                    if left_rows().peek().is_none() {
                        break;
                    }
                    left_row = left_rows().next().unwrap();
                }
                Ordering::Greater => {
                    if right_rows().peek().is_none() {
                        break;
                    }
                    right_row = right_rows().next().unwrap();
                }
                Ordering::Less => {
                    if left_rows().peek().is_none() {
                        break;
                    }
                    left_row = left_rows().next().unwrap();
                }
            }
        }
        // if rows line < PROCESSING_WINDOW_SIZE ,take rest rows out of builder
        if let Some(chunk) = { builder }.take() {
            yield chunk;
        }
    }
}
#[allow(dead_code)]
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
