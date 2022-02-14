// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use futures::Future;
use risinglight_proto::rowset::BlockIndex;

use super::{Column, ColumnIterator, ColumnSeekPosition};
use crate::array::{Array, ArrayBuilder, I64Array, I64ArrayBuilder};
use crate::storage::secondary::block::{BlockIterator, RowHandlerBlockIterator};
use crate::storage::StorageResult;

pub enum RowHandlerBlockIteratorImpl {
    RowHandler(RowHandlerBlockIterator),
}

impl BlockIterator<I64Array> for RowHandlerBlockIteratorImpl {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <I64Array as Array>::Builder,
    ) -> usize {
        match self {
            RowHandlerBlockIteratorImpl::RowHandler(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            RowHandlerBlockIteratorImpl::RowHandler(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            RowHandlerBlockIteratorImpl::RowHandler(it) => it.remaining_items(),
        }
    }
}

pub struct RowHandlerBlockIteratorFactory {}

impl RowHandlerBlockIteratorFactory {
    pub fn get_block_iterator(
        rowset_id: usize,
        index: &BlockIndex,
        start_pos: usize,
    ) -> RowHandlerBlockIteratorImpl {
        let mut it = RowHandlerBlockIterator::new(rowset_id, index.row_count as usize);
        it.skip(start_pos - index.first_rowid as usize);
        RowHandlerBlockIteratorImpl::RowHandler(it)
    }
}

pub struct RowHandlerIterator {
    /// The ID of the rowset corresponding to `Column`.
    rowset_id: u32,

    /// The [`Column`] object to iterate.
    column: Column,

    /// ID of the current block.
    current_block_id: u32,

    /// Block iterator.
    block_iterator: RowHandlerBlockIteratorImpl,

    /// RowID of the current column.
    current_row_id: u32,

    /// Indicates whether this iterator has finished or not.
    finished: bool,

    /// Indicate whether current_block_iter is fake.
    is_fake_iter: bool,
}

impl RowHandlerIterator {
    pub fn new(column: Column, start_pos: u32) -> Self {
        let current_block_id = column
            .index()
            .block_of_seek_position(ColumnSeekPosition::RowId(start_pos));
        let index = column.index().index(current_block_id);
        let rowset_id = column.base_block_key.rowset_id;
        Self {
            rowset_id,
            block_iterator: RowHandlerBlockIteratorFactory::get_block_iterator(
                rowset_id as usize,
                index,
                start_pos as usize,
            ),
            column,
            current_block_id,
            current_row_id: start_pos,
            finished: false,
            is_fake_iter: false,
        }
    }

    pub fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<(u32, I64Array)>> {
        if self.finished {
            return Ok(None);
        }

        let capacity = if let Some(expected_size) = expected_size {
            expected_size
        } else {
            self.block_iterator.remaining_items()
        };

        let mut builder = I64ArrayBuilder::with_capacity(capacity);
        let mut total_cnt = 0;
        let first_row_id = self.current_row_id;

        // Skip happened previously, we should forward to a new block first
        if self.is_fake_iter {
            self.is_fake_iter = false;
            let index = self.column.index().index(self.current_block_id);
            self.block_iterator = RowHandlerBlockIteratorFactory::get_block_iterator(
                self.rowset_id as usize,
                index,
                self.current_row_id as usize,
            );
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

            if self.incre_block_id() {
                break;
            }

            let index = self.column.index().index(self.current_block_id);
            self.block_iterator = RowHandlerBlockIteratorFactory::get_block_iterator(
                self.rowset_id as usize,
                index,
                self.current_row_id as usize,
            )
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

        // We are holding a fake iterator, so all the infomation can be
        // computed directly
        if self.is_fake_iter {
            let row_count = self.column.index().index(self.current_block_id).row_count;
            let start_pos = self.column.index().index(self.current_block_id).first_rowid;
            let mut reached = start_pos + row_count;
            while self.current_row_id > reached {
                if self.incre_block_id() {
                    return;
                }
                let row_count = self.column.index().index(self.current_block_id).row_count;
                reached += row_count;
            }
            return;
        }

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

        // Indicate that a new block (located by `current_block_id`) should be
        // loaded at the beginning of the next `next_batch`.
        self.is_fake_iter = true;
    }
}

impl ColumnIterator<I64Array> for RowHandlerIterator {
    type NextFuture<'a> = impl Future<Output = StorageResult<Option<(u32, I64Array)>>> + 'a;

    fn next_batch(&mut self, expected_size: Option<usize>) -> Self::NextFuture<'_> {
        async move { self.next_batch_inner(expected_size) }
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
