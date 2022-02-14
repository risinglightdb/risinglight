// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{Block, BlockIterator};
use super::{BlockIteratorFactory, ConcreteColumnIterator};
use crate::array::{BlobArray, BlobArrayBuilder};
use crate::storage::secondary::block::{FakeBlockIterator, PlainBlobBlockIterator};
use crate::types::BlobRef;

pub enum BlobBlockIteratorImpl {
    PlainBlob(PlainBlobBlockIterator<BlobRef>),
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
            Self::Fake(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::PlainBlob(it) => it.skip(cnt),
            Self::Fake(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::PlainBlob(it) => it.remaining_items(),
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
