// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{Block, BlockIterator};
use super::{BlockIteratorFactory, ConcreteColumnIterator};
use crate::array::{Utf8Array, Utf8ArrayBuilder};
use crate::storage::secondary::block::{
    FakeBlockIterator, PlainCharBlockIterator, PlainVarcharBlockIterator,
};

/// All supported block iterators for char types.
pub enum CharBlockIteratorImpl {
    PlainFixedChar(PlainCharBlockIterator),
    PlainVarchar(PlainVarcharBlockIterator),
    Fake(FakeBlockIterator<Utf8Array>),
}

impl BlockIterator<Utf8Array> for CharBlockIteratorImpl {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut Utf8ArrayBuilder,
    ) -> usize {
        match self {
            Self::PlainFixedChar(it) => it.next_batch(expected_size, builder),
            Self::PlainVarchar(it) => it.next_batch(expected_size, builder),
            Self::Fake(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::PlainFixedChar(it) => it.skip(cnt),
            Self::PlainVarchar(it) => it.skip(cnt),
            Self::Fake(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::PlainFixedChar(it) => it.remaining_items(),
            Self::PlainVarchar(it) => it.remaining_items(),
            Self::Fake(it) => it.remaining_items(),
        }
    }
}

pub struct CharBlockIteratorFactory {
    char_width: Option<usize>,
}

impl CharBlockIteratorFactory {
    pub fn new(char_width: Option<usize>) -> Self {
        Self { char_width }
    }
}

/// Column iterators on primitive types
pub type CharColumnIterator = ConcreteColumnIterator<Utf8Array, CharBlockIteratorFactory>;

impl BlockIteratorFactory<Utf8Array> for CharBlockIteratorFactory {
    type BlockIteratorImpl = CharBlockIteratorImpl;

    fn get_iterator_for(
        &self,
        block_type: BlockType,
        block: Block,
        index: &BlockIndex,
        start_pos: usize,
    ) -> Self::BlockIteratorImpl {
        let mut it = match (block_type, self.char_width) {
            (BlockType::PlainFixedChar, Some(char_width)) => {
                let it = PlainCharBlockIterator::new(block, index.row_count as usize, char_width);
                CharBlockIteratorImpl::PlainFixedChar(it)
            }
            (BlockType::PlainVarchar, _) => {
                let it = PlainVarcharBlockIterator::new(block, index.row_count as usize);
                CharBlockIteratorImpl::PlainVarchar(it)
            }
            _ => todo!(),
        };
        it.skip(start_pos - index.first_rowid as usize);
        it
    }

    fn get_fake_iterator(&self, index: &BlockIndex, start_pos: usize) -> Self::BlockIteratorImpl {
        let mut it = CharBlockIteratorImpl::Fake(FakeBlockIterator::new(index.row_count as usize));
        it.skip(start_pos - index.first_rowid as usize);
        it
    }
}
