// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::min;

use bitvec::prelude::BitVec;
use futures::Future;
use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{Block, BlockIterator};
use super::{Column, ColumnIterator, ColumnSeekPosition};
use crate::array::{Array, ArrayBuilder};
use crate::storage::StorageResult;

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

    /// Create a [`FakeBlockIterator`](super::super::block::FakeBlockIterator) from block index and
    /// seek to `start_pos`.
    fn get_fake_iterator(&self, index: &BlockIndex, start_pos: usize) -> Self::BlockIteratorImpl;
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

    /// Indicate whether current_block_iter is fake.
    is_fake_iter: bool,
}

impl<A: Array, F: BlockIteratorFactory<A>> ConcreteColumnIterator<A, F> {
    #[cfg(test)]
    pub fn get_current_row_id(&self) -> u32 {
        self.current_row_id
    }

    pub async fn new(column: Column, start_pos: u32, factory: F) -> StorageResult<Self> {
        let current_block_id = column
            .index()
            .block_of_seek_position(ColumnSeekPosition::RowId(start_pos));
        let (header, block) = column.get_block(current_block_id).await?;
        Ok(Self {
            block_iterator: factory.get_iterator_for(
                header.block_type,
                block,
                column.index().index(current_block_id),
                start_pos as usize,
            ),
            column,
            current_block_id,
            current_row_id: start_pos,
            finished: false,
            factory,
            is_fake_iter: false,
        })
    }

    pub async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<(u32, A)>> {
        if self.finished {
            return Ok(None);
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

            let (header, block) = self.column.get_block(self.current_block_id).await?;
            self.block_iterator = self.factory.get_iterator_for(
                header.block_type,
                block,
                self.column.index().index(self.current_block_id),
                self.current_row_id as usize,
            );
        }

        if total_cnt == 0 {
            Ok(None)
        } else {
            Ok(Some((first_row_id, builder.finish())))
        }
    }

    pub async fn next_batch_inner_with_filter(
        &mut self,
        expected_size: Option<usize>,
        filter_bitmap: &BitVec,
    ) -> StorageResult<Option<(u32, A)>> {
        if self.finished {
            return Ok(None);
        }

        let capacity = if let Some(expected_size) = expected_size {
            expected_size
        } else {
            self.block_iterator.remaining_items()
        };

        let mut builder = A::Builder::with_capacity(capacity);
        let mut total_cnt = 0;
        let first_row_id = self.current_row_id;

        if self.is_fake_iter {
            let count = min(filter_bitmap.len(), self.block_iterator.remaining_items());
            let subset = &filter_bitmap[..count];
            if !subset.not_any() {
                self.is_fake_iter = false;
                let (header, block) = self.column.get_block(self.current_block_id).await?;
                self.block_iterator = self.factory.get_iterator_for(
                    header.block_type,
                    block,
                    self.column.index().index(self.current_block_id),
                    self.current_row_id as usize,
                )
            }
        }

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

            self.is_fake_iter = false;

            if self.incre_block_id() {
                break;
            }

            let begin = (self.current_row_id - first_row_id) as usize;
            let count = min(
                self.column.index().index(self.current_block_id).row_count as usize,
                filter_bitmap.len() - begin,
            );
            let subset = &filter_bitmap[begin..begin + count];
            if subset.not_any() {
                self.is_fake_iter = true;
            }

            self.block_iterator = if self.is_fake_iter {
                self.factory.get_fake_iterator(
                    self.column.index().index(self.current_block_id),
                    self.current_row_id as usize,
                )
            } else {
                let (header, block) = self.column.get_block(self.current_block_id).await?;
                self.factory.get_iterator_for(
                    header.block_type,
                    block,
                    self.column.index().index(self.current_block_id),
                    self.current_row_id as usize,
                )
            }
        }

        if total_cnt == 0 {
            Ok(None)
        } else {
            Ok(Some((first_row_id, builder.finish())))
        }
    }

    fn fetch_hint_inner(&self) -> usize {
        if self.finished {
            return 0;
        }
        let index = self.column.index().index(self.current_block_id);
        (index.row_count - (self.current_row_id - index.first_rowid)) as usize
    }

    /// Increment the `current_block_id` by 1 and check whether it exceeds max block id.
    fn incre_block_id(&mut self) -> bool {
        let len = self.column.index().len() as u32;
        self.current_block_id += 1;
        if self.current_block_id >= len {
            self.finished = true;
            true
        } else {
            false
        }
    }

    fn skip_inner(&mut self, mut cnt: usize) {
        if self.finished {
            return;
        }
        self.current_row_id += cnt as u32;

        let remaining_items = self.block_iterator.remaining_items();
        if cnt >= remaining_items {
            cnt -= remaining_items;

            if self.incre_block_id() {
                return;
            }
        } else {
            self.block_iterator.skip(cnt);
            return;
        }

        while cnt > 0 {
            let row_count = self.column.index().index(self.current_block_id).row_count as usize;
            if cnt >= row_count {
                cnt -= row_count;

                if self.incre_block_id() {
                    return;
                }
            } else {
                cnt = 0;
            }
        }
        assert_eq!(cnt, 0);

        self.is_fake_iter = true;
        self.block_iterator = self.factory.get_fake_iterator(
            self.column.index().index(self.current_block_id),
            self.current_row_id as usize,
        )
    }
}

impl<A: Array, F: BlockIteratorFactory<A>> ColumnIterator<A> for ConcreteColumnIterator<A, F> {
    type NextFuture<'a> = impl Future<Output = StorageResult<Option<(u32, A)>>> + 'a;

    fn next_batch<'a>(
        &'a mut self,
        expected_size: Option<usize>,
        filter_bitmap: Option<&'a BitVec>,
    ) -> Self::NextFuture<'a> {
        async move {
            if let Some(fb) = filter_bitmap {
                self.next_batch_inner_with_filter(expected_size, fb).await
            } else {
                self.next_batch_inner(expected_size).await
            }
        }
    }
    fn fetch_hint(&self) -> usize {
        self.fetch_hint_inner()
    }

    fn fetch_current_row_id(&self) -> u32 {
        self.current_row_id
    }

    fn skip(&mut self, cnt: usize) {
        self.skip_inner(cnt);
    }
}
