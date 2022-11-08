use std::marker::PhantomData;

use bitvec::prelude::Lsb0;
use bitvec::slice::BitSlice;
use bitvec::vec::BitVec;
use bytes::Buf;

use super::{Block, BlockIterator, NonNullableBlockIterator};
use crate::array::{Array, ArrayBuilder};

pub fn decode_nullable_block(data: Block) -> (Block, Block) {
    let mut bitmap_len_buf = &data[data.len() - 4..];
    let bitmap_len = bitmap_len_buf.get_u32_le() as usize;
    let bitmap_block = data.slice(data.len() - 4 - bitmap_len..data.len() - 4);
    let inner_block = data.slice(..data.len() - 4 - bitmap_len);
    (inner_block, bitmap_block)
}

pub struct NullableBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A> + NonNullableBlockIterator<A>,
{
    /// Inner block iterator
    inner_iter: B,
    /// Indicates current position in the block
    cur_row: usize,
    bitmap_block: Block,
    _phantom: PhantomData<A>,
}

impl<A, B> NullableBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A> + NonNullableBlockIterator<A>,
{
    pub fn new(inner_iter: B, bitmap_block: Block) -> Self {
        Self {
            inner_iter,
            cur_row: 0,
            bitmap_block,
            _phantom: PhantomData,
        }
    }
}

impl<A, B> BlockIterator<A> for NullableBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A> + NonNullableBlockIterator<A>,
{
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <A as Array>::Builder,
    ) -> usize {
        let inner_result = self.inner_iter.next_batch_non_null(expected_size, builder);

        let bitmap_slice = &BitSlice::<u8, Lsb0>::from_slice(&self.bitmap_block)
            [self.cur_row..self.cur_row + inner_result];
        let mut bitmap_for_builder: BitVec = BitVec::with_capacity(bitmap_slice.len());
        bitmap_slice
            .iter()
            .for_each(|x| bitmap_for_builder.push(*x));
        builder.replace_bitmap(bitmap_for_builder);
        self.cur_row += inner_result;
        inner_result
    }

    fn skip(&mut self, cnt: usize) {
        self.inner_iter.skip(cnt);
        self.cur_row += cnt;
    }

    fn remaining_items(&self) -> usize {
        self.inner_iter.remaining_items()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::array::{ArrayToVecExt, I32ArrayBuilder, Utf8ArrayBuilder};
    use crate::storage::secondary::block::{
        BlockBuilder, NullableBlockBuilder, PlainBlobBlockBuilder, PlainBlobBlockIterator,
        PlainPrimitiveBlockBuilder, PlainPrimitiveBlockIterator,
    };

    #[test]
    fn test_scan_i32() {
        let inner_builder = PlainPrimitiveBlockBuilder::<i32>::new(128);
        let mut builder = NullableBlockBuilder::new(inner_builder, 128);
        builder.append(Some(&1));
        builder.append(None);
        builder.append(Some(&3));
        let data = builder.finish();

        let (inner_block, bitmap_block) = decode_nullable_block(Bytes::from(data));
        let inner_iter = PlainPrimitiveBlockIterator::<i32>::new(inner_block, 3);
        let mut iter = NullableBlockIterator::new(inner_iter, bitmap_block);
        iter.skip(1);
        assert_eq!(iter.remaining_items(), 2);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(iter.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![None]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(iter.next_batch(Some(2), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(iter.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_build_varchar() {
        let inner_builder = PlainBlobBlockBuilder::<str>::new(128);
        let mut builder = NullableBlockBuilder::new(inner_builder, 128);

        builder.append(Some("233"));
        builder.append(None);
        builder.append(Some("2333333"));
        let data = builder.finish();

        let (inner_block, bitmap_block) = decode_nullable_block(Bytes::from(data));
        let inner_iter = PlainBlobBlockIterator::<str>::new(inner_block, 3);
        let mut iter = NullableBlockIterator::new(inner_iter, bitmap_block);
        iter.skip(1);
        assert_eq!(iter.remaining_items(), 2);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(iter.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![None]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(iter.next_batch(Some(2), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some("2333333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(iter.next_batch(None, &mut builder), 0);
    }
}
