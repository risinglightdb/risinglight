// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{Block, BlockIterator};
use super::{BlockIteratorFactory, ConcreteColumnIterator};
use crate::array::{ArrayBuilder, BlobArray, BlobArrayBuilder};
use crate::storage::secondary::block::{
    decode_dict_block, decode_nullable_block, decode_rle_block, DictBlockIterator,
    FakeBlockIterator, NullableBlockIterator, PlainBlobBlockIterator, RleBlockIterator,
};
use crate::types::BlobRef;

type PlainNullableBlobBlockIterator =
    NullableBlockIterator<BlobArray, PlainBlobBlockIterator<BlobRef>>;

pub enum BlobBlockIteratorImpl {
    Plain(PlainBlobBlockIterator<BlobRef>),
    PlainNullable(PlainNullableBlobBlockIterator),
    RunLength(RleBlockIterator<BlobArray, PlainBlobBlockIterator<BlobRef>>),
    RleNullable(RleBlockIterator<BlobArray, PlainNullableBlobBlockIterator>),
    Dictionary(DictBlockIterator<BlobArray, PlainBlobBlockIterator<BlobRef>>),
    DictNullable(DictBlockIterator<BlobArray, PlainNullableBlobBlockIterator>),
    Fake(FakeBlockIterator<BlobArray>),
}

impl BlockIterator<BlobArray> for BlobBlockIteratorImpl {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut BlobArrayBuilder,
    ) -> usize {
        match self {
            Self::Plain(it) => it.next_batch(expected_size, builder),
            Self::PlainNullable(it) => it.next_batch(expected_size, builder),
            Self::RunLength(it) => it.next_batch(expected_size, builder),
            Self::RleNullable(it) => it.next_batch(expected_size, builder),
            Self::Dictionary(it) => it.next_batch(expected_size, builder),
            Self::DictNullable(it) => it.next_batch(expected_size, builder),
            Self::Fake(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::Plain(it) => it.skip(cnt),
            Self::PlainNullable(it) => it.skip(cnt),
            Self::RunLength(it) => it.skip(cnt),
            Self::RleNullable(it) => it.skip(cnt),
            Self::Dictionary(it) => it.skip(cnt),
            Self::DictNullable(it) => it.skip(cnt),
            Self::Fake(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::Plain(it) => it.remaining_items(),
            Self::PlainNullable(it) => it.remaining_items(),
            Self::RunLength(it) => it.remaining_items(),
            Self::RleNullable(it) => it.remaining_items(),
            Self::Dictionary(it) => it.remaining_items(),
            Self::DictNullable(it) => it.remaining_items(),
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
            BlockType::Plain => BlobBlockIteratorImpl::Plain(PlainBlobBlockIterator::new(
                block,
                index.row_count as usize,
            )),
            BlockType::PlainNullable => {
                let (inner, bitmap) = decode_nullable_block(block);
                let block_iter = PlainBlobBlockIterator::new(inner, index.row_count as usize);
                BlobBlockIteratorImpl::PlainNullable(NullableBlockIterator::new(block_iter, bitmap))
            }
            BlockType::RunLength => {
                let (rle_num, rle_data, block_data) = decode_rle_block(block);
                let block_iter = PlainBlobBlockIterator::new(block_data, rle_num);
                let it = RleBlockIterator::<BlobArray, PlainBlobBlockIterator<BlobRef>>::new(
                    block_iter, rle_data, rle_num,
                );
                BlobBlockIteratorImpl::RunLength(it)
            }
            BlockType::RleNullable => {
                let (rle_num, rle_data, block_data) = decode_rle_block(block);
                let (inner, bitmap) = decode_nullable_block(block_data);
                let block_iter = PlainBlobBlockIterator::new(inner, rle_num);
                let nullable_iter = NullableBlockIterator::new(block_iter, bitmap);
                let it = RleBlockIterator::new(nullable_iter, rle_data, rle_num);
                BlobBlockIteratorImpl::RleNullable(it)
            }
            BlockType::Dictionary => {
                let mut dict_builder = BlobArrayBuilder::new();
                let (dict_count_sum, dict_block, rle_block) = decode_dict_block(block);
                let mut dict_iter = PlainBlobBlockIterator::new(dict_block, dict_count_sum);
                let it = DictBlockIterator::<BlobArray, PlainBlobBlockIterator<BlobRef>>::new(
                    &mut dict_builder,
                    &mut dict_iter,
                    rle_block,
                    dict_count_sum,
                );
                BlobBlockIteratorImpl::Dictionary(it)
            }
            BlockType::DictNullable => {
                let mut dict_builder = BlobArrayBuilder::new();
                let (dict_count_sum, dict_block, rle_block) = decode_dict_block(block);
                let (inner, bitmap) = decode_nullable_block(dict_block);
                let mut nullable_dict_iter = NullableBlockIterator::new(
                    PlainBlobBlockIterator::new(inner, dict_count_sum),
                    bitmap,
                );
                let it = DictBlockIterator::new(
                    &mut dict_builder,
                    &mut nullable_dict_iter,
                    rle_block,
                    dict_count_sum,
                );
                BlobBlockIteratorImpl::DictNullable(it)
            }
            _ => unreachable!(),
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
