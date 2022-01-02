use std::cmp::min;

use async_trait::async_trait;
use bitvec::prelude::BitVec;
use bytes::Bytes;
use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{Block, BlockIterator};
use super::{Column, ColumnIterator, ColumnSeekPosition};
use crate::array::{Array, ArrayBuilder};

/// Unifying all iterators of a given type
pub trait BlockIteratorFactory<A: Array>: Send + Sync + 'static {
    /// Generally an enum for all supported iterators for a concrete type
    type BlockIteratorImpl: BlockIterator<A> + Send + 'static;

    /// Create iterator from block type, block index and block content, and seek to `start_pos`.
    fn get_iterator_for(
        &self,
        block_type: BlockType,
        block: Block,
        index: &BlockIndex,
        start_pos: usize,
    ) -> Self::BlockIteratorImpl;
}

/// Column iterator that operates on a concrete type
pub struct ConcreteColumnIterator<A: Array, F: BlockIteratorFactory<A>> {
    /// The [`Column`] object to iterate.
    column: Column,

    /// ID of the current block.
    current_block_id: u32,

    /// Block iterator.
    block_iterator: F::BlockIteratorImpl,

    /// RowID of the current column.
    current_row_id: u32,

    /// Indicates whether this iterator has finished or not.
    finished: bool,

    /// The factory for creating iterators.
    factory: F,

    /// Block Type of this column.
    block_type: BlockType,

    /// Indicate whether current_block_iter is fake.
    is_fake_iter: bool,

    start_row_id: u32,
}

impl<A: Array, F: BlockIteratorFactory<A>> ConcreteColumnIterator<A, F> {
    pub async fn new(column: Column, start_pos: u32, factory: F) -> Self {
        let current_block_id = column
            .index()
            .block_of_seek_position(ColumnSeekPosition::RowId(start_pos));
        let (header, block) = column.get_block(current_block_id).await;
        let block_type = header.block_type;
        Self {
            block_iterator: factory.get_iterator_for(
                block_type,
                block,
                column.index().index(current_block_id),
                start_pos as usize,
            ),
            column,
            current_block_id,
            current_row_id: start_pos,
            finished: false,
            factory,
            block_type,
            is_fake_iter: false,
            start_row_id: start_pos,
        }
    }

    pub async fn next_batch_inner(&mut self, expected_size: Option<usize>) -> Option<(u32, A)> {
        if self.finished {
            return None;
        }

        let capacity = if let Some(expected_size) = expected_size {
            expected_size
        } else {
            self.block_iterator.remaining_items()
        };

        let mut builder = A::Builder::with_capacity(capacity);
        let mut total_cnt = 0;
        let first_row_id = self.current_row_id;

        loop {
            let cnt = self
                .block_iterator
                .next_batch(expected_size.map(|x| x - total_cnt), &mut builder);

            total_cnt += cnt;
            self.current_row_id += cnt as u32;

            if let Some(expected_size) = expected_size {
                if total_cnt >= expected_size {
                    break;
                }
            } else if total_cnt != 0 {
                break;
            }

            self.current_block_id += 1;

            if self.current_block_id >= self.column.index().len() as u32 {
                self.finished = true;
                break;
            }

            let (header, block) = self.column.get_block(self.current_block_id).await;
            self.block_iterator = self.factory.get_iterator_for(
                header.block_type,
                block,
                self.column.index().index(self.current_block_id),
                self.current_row_id as usize,
            );
        }

        if total_cnt == 0 {
            None
        } else {
            Some((first_row_id, builder.finish()))
        }
    }

    pub async fn next_batch_inner_with_filter(
        &mut self,
        expected_size: Option<usize>,
        filter_bitmap: &BitVec,
    ) -> Option<(u32, A)> {
        if self.finished {
            return None;
        }

        let capacity = if let Some(expected_size) = expected_size {
            expected_size
        } else {
            self.block_iterator.remaining_items()
        };

        let mut builder = A::Builder::with_capacity(capacity);
        let mut total_cnt = 0;
        let first_row_id = self.current_row_id;

        loop {
            let cnt = if self.is_fake_iter {
                let mut count = self.block_iterator.remaining_items();
                if let Some(expected_size) = expected_size {
                    count = min(expected_size, count);
                }
                self.block_iterator.skip(count);
                for _ in 0..count {
                    builder.push(None);
                }
                count
            } else {
                self.block_iterator
                    .next_batch(expected_size.map(|x| x - total_cnt), &mut builder)
            };

            total_cnt += cnt;
            self.current_row_id += cnt as u32;

            if let Some(expected_size) = expected_size {
                if total_cnt >= expected_size {
                    break;
                }
            } else if total_cnt != 0 {
                break;
            }

            self.current_block_id += 1;
            self.is_fake_iter = false;

            if self.current_block_id >= self.column.index().len() as u32 {
                self.finished = true;
                break;
            }

            // let count = min(self.column.index().index(self.current_block_id).row_count as usize,
            // filter_bitmap.len()); let subset = &filter_bitmap[0..count];
            // if subset.not_any() {
            //     self.is_fake_iter = true;
            // }
            // *filter_bitmap = filter_bitmap.split_off(count);
            let begin = (self.current_row_id - self.start_row_id) as usize;
            let count = min(
                self.column.index().index(self.current_block_id).row_count as usize,
                filter_bitmap.len() - begin,
            );
            let subset = &filter_bitmap[begin..begin + count];
            if subset.not_any() {
                self.is_fake_iter = true;
            }

            let block = if self.is_fake_iter {
                Bytes::new()
            } else {
                self.column.get_block(self.current_block_id).await.1
            };
            self.block_iterator = self.factory.get_iterator_for(
                self.block_type,
                block,
                self.column.index().index(self.current_block_id),
                self.current_row_id as usize,
            )
        }

        if total_cnt == 0 {
            None
        } else {
            Some((first_row_id, builder.finish()))
        }
    }
    fn fetch_hint_inner(&self) -> usize {
        if self.finished {
            return 0;
        }
        let index = self.column.index().index(self.current_block_id);
        (index.row_count - (self.current_row_id - index.first_rowid)) as usize
    }
}

#[async_trait]
impl<A: Array, F: BlockIteratorFactory<A>> ColumnIterator<A> for ConcreteColumnIterator<A, F> {
    async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        filter_bitmap: Option<&BitVec>,
    ) -> Option<(u32, A)> {
        if let Some(fb) = filter_bitmap {
            self.next_batch_inner_with_filter(expected_size, fb).await
        } else {
            self.next_batch_inner(expected_size).await
        }
    }
    fn fetch_hint(&self) -> usize {
        self.fetch_hint_inner()
    }
}
