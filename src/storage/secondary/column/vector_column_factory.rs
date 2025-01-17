// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{Block, BlockIterator};
use super::{BlockIteratorFactory, ConcreteColumnIterator};
use crate::array::{VectorArray, VectorArrayBuilder};
use crate::storage::secondary::block::{
    decode_nullable_block, FakeBlockIterator, NullableBlockIterator,
};
use crate::storage::secondary::PlainVectorBlockIterator;

type PlainNullableVectorBlockIterator =
    NullableBlockIterator<VectorArray, PlainVectorBlockIterator>;

pub enum VectorBlockIteratorImpl {
    Plain(PlainVectorBlockIterator),
    PlainNullable(PlainNullableVectorBlockIterator),
    Fake(FakeBlockIterator<VectorArray>),
}

impl BlockIterator<VectorArray> for VectorBlockIteratorImpl {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut VectorArrayBuilder,
    ) -> usize {
        match self {
            Self::Plain(it) => it.next_batch(expected_size, builder),
            Self::PlainNullable(it) => it.next_batch(expected_size, builder),
            Self::Fake(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::Plain(it) => it.skip(cnt),
            Self::PlainNullable(it) => it.skip(cnt),
            Self::Fake(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::Plain(it) => it.remaining_items(),
            Self::PlainNullable(it) => it.remaining_items(),
            Self::Fake(it) => it.remaining_items(),
        }
    }
}

pub struct VectorBlockIteratorFactory();

/// Column iterators on primitive types
pub type VectorColumnIterator = ConcreteColumnIterator<VectorArray, VectorBlockIteratorFactory>;

impl BlockIteratorFactory<VectorArray> for VectorBlockIteratorFactory {
    type BlockIteratorImpl = VectorBlockIteratorImpl;

    fn get_iterator_for(
        &self,
        block_type: BlockType,
        block: Block,
        index: &BlockIndex,
        start_pos: usize,
    ) -> Self::BlockIteratorImpl {
        let mut it = match block_type {
            BlockType::Plain => VectorBlockIteratorImpl::Plain(PlainVectorBlockIterator::new(
                block,
                index.row_count as usize,
            )),
            BlockType::PlainNullable => {
                let (inner, bitmap) = decode_nullable_block(block);
                let block_iter = PlainVectorBlockIterator::new(inner, index.row_count as usize);
                VectorBlockIteratorImpl::PlainNullable(NullableBlockIterator::new(
                    block_iter, bitmap,
                ))
            }
            _ => unimplemented!(),
        };
        it.skip(start_pos - index.first_rowid as usize);
        it
    }

    fn get_fake_iterator(&self, index: &BlockIndex, start_pos: usize) -> Self::BlockIteratorImpl {
        let mut it =
            VectorBlockIteratorImpl::Fake(FakeBlockIterator::new(index.row_count as usize));
        it.skip(start_pos - index.first_rowid as usize);
        it
    }
}
