// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{Block, BlockIterator};
use super::{BlockIteratorFactory, ConcreteColumnIterator};
use crate::array::{ArrayBuilder, Utf8Array, Utf8ArrayBuilder};
use crate::storage::secondary::block::{
    decode_dict_block, decode_rle_block, DictBlockIterator, FakeBlockIterator,
    PlainBlobBlockIterator, PlainCharBlockIterator, RleBlockIterator,
};

/// All supported block iterators for char types.
pub enum CharBlockIteratorImpl {
    PlainFixedChar(PlainCharBlockIterator),
    PlainVarchar(PlainBlobBlockIterator<str>),
    RleFixedChar(RleBlockIterator<Utf8Array, PlainCharBlockIterator>),
    RleVarchar(RleBlockIterator<Utf8Array, PlainBlobBlockIterator<str>>),
    DictFixedChar(DictBlockIterator<Utf8Array, PlainCharBlockIterator>),
    DictVarchar(DictBlockIterator<Utf8Array, PlainBlobBlockIterator<str>>),
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
            Self::RleFixedChar(it) => it.next_batch(expected_size, builder),
            Self::RleVarchar(it) => it.next_batch(expected_size, builder),
            Self::DictFixedChar(it) => it.next_batch(expected_size, builder),
            Self::DictVarchar(it) => it.next_batch(expected_size, builder),
            Self::Fake(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::PlainFixedChar(it) => it.skip(cnt),
            Self::PlainVarchar(it) => it.skip(cnt),
            Self::RleFixedChar(it) => it.skip(cnt),
            Self::RleVarchar(it) => it.skip(cnt),
            Self::DictFixedChar(it) => it.skip(cnt),
            Self::DictVarchar(it) => it.skip(cnt),
            Self::Fake(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::PlainFixedChar(it) => it.remaining_items(),
            Self::PlainVarchar(it) => it.remaining_items(),
            Self::RleFixedChar(it) => it.remaining_items(),
            Self::RleVarchar(it) => it.remaining_items(),
            Self::DictFixedChar(it) => it.remaining_items(),
            Self::DictVarchar(it) => it.remaining_items(),
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
                let it = PlainBlobBlockIterator::new(block, index.row_count as usize);
                CharBlockIteratorImpl::PlainVarchar(it)
            }
            (BlockType::RleFixedChar, Some(char_width)) => {
                let (rle_num, rle_data, block_data) = decode_rle_block(block);
                let block_iter = PlainCharBlockIterator::new(block_data, rle_num, char_width);
                let it = RleBlockIterator::<Utf8Array, PlainCharBlockIterator>::new(
                    block_iter, rle_data, rle_num,
                );
                CharBlockIteratorImpl::RleFixedChar(it)
            }
            (BlockType::RleVarchar, _) => {
                let (rle_num, rle_data, block_data) = decode_rle_block(block);
                let block_iter = PlainBlobBlockIterator::new(block_data, rle_num);
                let it = RleBlockIterator::<Utf8Array, PlainBlobBlockIterator<str>>::new(
                    block_iter, rle_data, rle_num,
                );
                CharBlockIteratorImpl::RleVarchar(it)
            }
            (BlockType::DictFixedChar, Some(char_width)) => {
                let mut dict_builder = Utf8ArrayBuilder::new();
                let (dict_count_sum, dict_block, rle_block) = decode_dict_block(block);
                let mut dict_iter =
                    PlainCharBlockIterator::new(dict_block, dict_count_sum, char_width);
                let it = DictBlockIterator::<Utf8Array, PlainCharBlockIterator>::new(
                    &mut dict_builder,
                    &mut dict_iter,
                    rle_block,
                    dict_count_sum,
                );
                CharBlockIteratorImpl::DictFixedChar(it)
            }
            (BlockType::DictVarchar, _) => {
                let mut dict_builder = Utf8ArrayBuilder::new();
                let (dict_count_sum, dict_block, rle_block) = decode_dict_block(block);
                let mut dict_iter = PlainBlobBlockIterator::new(dict_block, dict_count_sum);
                let it = DictBlockIterator::<Utf8Array, PlainBlobBlockIterator<str>>::new(
                    &mut dict_builder,
                    &mut dict_iter,
                    rle_block,
                    dict_count_sum,
                );
                CharBlockIteratorImpl::DictVarchar(it)
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
