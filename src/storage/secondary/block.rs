// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Secondary's Block builders and iterators
//!
//! [`Block`] is the minimum managing unit in the storage engine.

mod blob_block_builder;
mod blob_block_iterator;
mod char_block_builder;
mod dict_block_builder;
mod dict_block_iterator;
mod fake_block_iterator;
mod nullable_block_builder;
mod nullable_block_iterator;
mod primitive_block_builder;
mod primitive_block_iterator;
mod rle_block_builder;
mod rle_block_iterator;
mod vector_block_builder;
mod vector_block_iterator;

use bitvec::prelude::{BitVec, Lsb0};
pub use blob_block_builder::*;
pub use blob_block_iterator::*;
pub use char_block_builder::*;
pub use fake_block_iterator::*;
pub use nullable_block_builder::*;
pub use primitive_block_builder::*;
pub use primitive_block_iterator::*;
use risinglight_proto::rowset::BlockStatistics;
mod char_block_iterator;
pub use char_block_iterator::*;
pub use dict_block_builder::*;
pub use dict_block_iterator::*;
pub use nullable_block_iterator::*;
pub use rle_block_builder::*;
pub use rle_block_iterator::*;
mod block_index_builder;
pub use block_index_builder::*;
use bytes::{Buf, BufMut, Bytes};
use risinglight_proto::rowset::block_checksum::ChecksumType;
use risinglight_proto::rowset::block_index::BlockType;
pub use vector_block_builder::*;
pub use vector_block_iterator::*;

use super::StorageResult;
use crate::array::Array;
use crate::storage::TracedStorageError;

/// A block is simply a [`Bytes`] array.
pub type Block = Bytes;

/// Builds a block. All builders should implement the trait, while
/// ensuring that the format follows the block encoding scheme.
///
/// In RisingLight, the block encoding scheme is as follows:
///
/// ```plain
/// |    data     | block_type | cksum_type | cksum  |
/// |  variable   |    4B      |     4B     |   8B   |
/// ```
pub trait BlockBuilder<A: Array> {
    /// Append one data into the block, or default/null value if item is None
    fn append(&mut self, item: Option<&A::Item>);

    /// Get estimated size of block. Will be useful on runlength or compression encoding.
    fn estimated_size(&self) -> usize;

    /// Get statistics of block
    fn get_statistics(&self) -> Vec<BlockStatistics>;

    /// Check if we should finish the current block. If there is no item in the current
    /// builder, this function must return `true`.
    fn should_finish(&self, next_item: &Option<&A::Item>) -> bool;

    /// Finish a block and return encoded data.
    fn finish(self) -> Vec<u8>;

    /// Get target size of block.
    fn get_target_size(&self) -> usize;
}

pub trait NonNullableBlockBuilder<A: Array> {
    fn append_value(&mut self, item: &A::Item);

    fn append_default(&mut self);

    /// Get statistics with selection bit vector. Select all values
    /// if `selection` is empty
    fn get_statistics_with_bitmap(&self, selection: &BitVec<u8, Lsb0>) -> Vec<BlockStatistics>;
    /// Get estimated size if append `next_item`.
    fn estimated_size_with_next_item(&self, next_item: &Option<&A::Item>) -> usize;
    /// Return true if no element in builder
    fn is_empty(&self) -> bool;
}

/// An iterator on a block. This iterator requires the block being pre-loaded in memory.
pub trait BlockIterator<A: Array> {
    /// Get a batch from the block. A `0` return value means that this batch contains no
    /// element. Some iterators might support exact size output. By using `expected_size`,
    /// developers can get an array of NO MORE THAN the `expected_size`.
    fn next_batch(&mut self, expected_size: Option<usize>, builder: &mut A::Builder) -> usize;

    /// Skip `cnt` items.
    fn skip(&mut self, cnt: usize);

    /// Number of items remaining in this block
    fn remaining_items(&self) -> usize;
}

pub trait NonNullableBlockIterator<A: Array> {
    /// Get a batch from the block. A `0` return value means that this batch contains no
    /// element. Some iterators might support exact size output. By using `expected_size`,
    /// developers can get an array of NO MORE THAN the `expected_size`.
    fn next_batch_non_null(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut A::Builder,
    ) -> usize;
}

/// A key in block cache contains `rowset_id`, `column_id` and `block_id`.
///
/// TODO: support per-table self-increment RowSet Id. Currently, all tables share one RowSet ID
/// generator.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct BlockCacheKey {
    pub rowset_id: u32,
    pub storage_column_id: u32,
    pub block_id: u32,
}

impl BlockCacheKey {
    pub fn block(mut self, block_id: u32) -> Self {
        self.block_id = block_id;
        self
    }

    pub fn column(mut self, storage_column_id: u32) -> Self {
        self.storage_column_id = storage_column_id;
        self
    }

    pub fn rowset(mut self, rowset_id: u32) -> Self {
        self.rowset_id = rowset_id;
        self
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockMeta {
    pub block_type: BlockType,
    pub checksum_type: ChecksumType,
    pub checksum: u64,
}

pub const BLOCK_META_NON_CHECKSUM_SIZE: usize = 4;
pub const BLOCK_META_CHECKSUM_SIZE: usize = 4 + 8;
pub const BLOCK_META_SIZE: usize = BLOCK_META_NON_CHECKSUM_SIZE + BLOCK_META_CHECKSUM_SIZE;

impl BlockMeta {
    pub fn encode_except_checksum(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.block_type.into());
    }

    pub fn encode_checksum(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.checksum_type.into());
        buf.put_u64(self.checksum);
    }

    pub fn decode(&mut self, buf: &mut impl Buf) -> StorageResult<()> {
        if buf.remaining() < 4 + 4 + 8 {
            return Err(TracedStorageError::decode("expected 16 bytes"));
        }
        self.block_type = BlockType::try_from(buf.get_i32())
            .map_err(|_| TracedStorageError::decode("expected valid block type"))?;
        self.checksum_type = ChecksumType::try_from(buf.get_i32())
            .map_err(|_| TracedStorageError::decode("expected valid checksum type"))?;
        self.checksum = buf.get_u64();
        Ok(())
    }
}
