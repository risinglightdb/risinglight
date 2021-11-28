use crate::array::{ArrayBuilder, Utf8Array, Utf8ArrayBuilder};
use crate::storage::secondary::block::PlainCharBlockIterator;

use super::super::{Block, BlockIterator};
use super::{Column, ColumnIterator, ColumnSeekPosition};

use async_trait::async_trait;
use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

/// All supported block iterators for char types.
pub(super) enum PlainCharBlockIteratorImpl {
    Plain(PlainCharBlockIterator),
}

pub struct CharColumnIterator {
    column: Column,
    current_block_id: u32,
    block_iterator: PlainCharBlockIteratorImpl,
    current_row_id: u32,
    finished: bool,
    char_width: Option<usize>,
}

impl CharColumnIterator {
    fn get_iterator_for(
        block_type: BlockType,
        block: Block,
        index: &BlockIndex,
        start_pos: usize,
        char_width: Option<usize>,
    ) -> PlainCharBlockIteratorImpl {
        match (block_type, char_width) {
            (BlockType::PlainFixedChar, Some(char_width)) => {
                let mut it =
                    PlainCharBlockIterator::new(block, index.row_count as usize, char_width);
                it.skip(start_pos - index.first_rowid as usize);
                PlainCharBlockIteratorImpl::Plain(it)
            }
            _ => todo!(),
        }
    }

    pub async fn new(column: Column, start_pos: u32, char_width: Option<usize>) -> Self {
        let current_block_id = column
            .index()
            .block_of_seek_position(ColumnSeekPosition::RowId(start_pos));
        let (header, block) = column.get_block(current_block_id).await;

        Self {
            block_iterator: Self::get_iterator_for(
                header.block_type,
                block,
                column.index().index(current_block_id),
                start_pos as usize,
                char_width,
            ),
            column,
            current_block_id,
            current_row_id: start_pos,
            finished: false,
            char_width,
        }
    }

    pub async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> Option<(u32, Utf8Array)> {
        if self.finished {
            return None;
        }

        let capacity = if let Some(expected_size) = expected_size {
            expected_size
        } else {
            match &mut self.block_iterator {
                PlainCharBlockIteratorImpl::Plain(bi) => bi.remaining_items(),
            }
        };

        let mut builder = Utf8ArrayBuilder::new(capacity);
        let mut total_cnt = 0;
        let first_row_id = self.current_row_id;

        loop {
            let cnt = match &mut self.block_iterator {
                PlainCharBlockIteratorImpl::Plain(bi) => {
                    bi.next_batch(expected_size.map(|x| x - total_cnt), &mut builder)
                }
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

            if self.current_block_id >= self.column.index().len() as u32 {
                self.finished = true;
                break;
            }

            let (header, block) = self.column.get_block(self.current_block_id).await;
            self.block_iterator = Self::get_iterator_for(
                header.block_type,
                block,
                self.column.index().index(self.current_block_id),
                self.current_row_id as usize,
                self.char_width,
            );
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
impl ColumnIterator<Utf8Array> for CharColumnIterator {
    async fn next_batch(&mut self, expected_size: Option<usize>) -> Option<(u32, Utf8Array)> {
        self.next_batch_inner(expected_size).await
    }

    fn fetch_hint(&self) -> usize {
        self.fetch_hint_inner()
    }
}
