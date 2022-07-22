// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{Block, BlockIterator};
use super::{BlockIteratorFactory, ConcreteColumnIterator};
use crate::array::{ArrayBuilder, BlobArray, BlobArrayBuilder};
use crate::storage::secondary::block::{
    decode_dict_block, decode_rle_block, DictBlockIterator, FakeBlockIterator,
    PlainBlobBlockIterator, RleBlockIterator,
};
use crate::types::BlobRef;

pub enum BlobBlockIteratorImpl {
    PlainBlob(PlainBlobBlockIterator<BlobRef>),
    RleBlob(RleBlockIterator<BlobArray, PlainBlobBlockIterator<BlobRef>>),
    DictBlob(DictBlockIterator<BlobArray, PlainBlobBlockIterator<BlobRef>>),
    Fake(FakeBlockIterator<BlobArray>),
}

impl BlockIterator<BlobArray> for BlobBlockIteratorImpl {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut BlobArrayBuilder,
    ) -> usize {
        match self {
            Self::PlainBlob(it) => it.next_batch(expected_size, builder),
            Self::RleBlob(it) => it.next_batch(expected_size, builder),
            Self::DictBlob(it) => it.next_batch(expected_size, builder),
            Self::Fake(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::PlainBlob(it) => it.skip(cnt),
            Self::RleBlob(it) => it.skip(cnt),
            Self::DictBlob(it) => it.skip(cnt),
            Self::Fake(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::PlainBlob(it) => it.remaining_items(),
            Self::RleBlob(it) => it.remaining_items(),
            Self::DictBlob(it) => it.remaining_items(),
            Self::Fake(it) => it.remaining_items(),
        }
    }
}

pub struct BlobBlockIteratorFactory();

/// Column iterators on primitive types
pub type BlobColumnIterator = ConcreteColumnIterator<BlobArray, BlobBlockIteratorFactory>;

impl BlockIteratorFactory<BlobArray> for BlobBlockIteratorFactory {
    type BlockIteratorImpl = BlobBlockIteratorImpl;

    fn get_iterator_for(
        &self,
        block_type: BlockType,
        block: Block,
        index: &BlockIndex,
        start_pos: usize,
    ) -> Self::BlockIteratorImpl {
        let mut it = match block_type {
            BlockType::PlainVarchar => BlobBlockIteratorImpl::PlainBlob(
                PlainBlobBlockIterator::new(block, index.row_count as usize),
            ),
            BlockType::RleVarchar => {
                let (rle_num, rle_data, block_data) = decode_rle_block(block);
                let block_iter = PlainBlobBlockIterator::new(block_data, rle_num);
                let it = RleBlockIterator::<BlobArray, PlainBlobBlockIterator<BlobRef>>::new(
                    block_iter, rle_data, rle_num,
                );
                BlobBlockIteratorImpl::RleBlob(it)
            }
            BlockType::DictVarchar => {
                let mut dict_builder = BlobArrayBuilder::new();
                let (dict_count_sum, dict_block, rle_block) = decode_dict_block(block);
                let mut dict_iter = PlainBlobBlockIterator::new(dict_block, dict_count_sum);
                let it = DictBlockIterator::<BlobArray, PlainBlobBlockIterator<BlobRef>>::new(
                    &mut dict_builder,
                    &mut dict_iter,
                    rle_block,
                    dict_count_sum,
                );
                BlobBlockIteratorImpl::DictBlob(it)
            }
            _ => todo!(),
        };
        it.skip(start_pos - index.first_rowid as usize);
        it
    }

    fn get_fake_iterator(&self, index: &BlockIndex, start_pos: usize) -> Self::BlockIteratorImpl {
        let mut it = BlobBlockIteratorImpl::Fake(FakeBlockIterator::new(index.row_count as usize));
        it.skip(start_pos - index.first_rowid as usize);
        it
    }
}
