use std::marker::PhantomData;

use bitvec::prelude::{BitVec, Lsb0};
use bytes::BufMut;
use risinglight_proto::rowset::BlockStatistics;

use super::{BlockBuilder, NonNullableBlockBuilder};
use crate::array::Array;

pub struct NullableBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A> + NonNullableBlockBuilder<A>,
{
    inner_builder: B,
    bitmap: BitVec<u8, Lsb0>,
    target_size: usize,
    _phantom: PhantomData<A>,
}

impl<A, B> NullableBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A> + NonNullableBlockBuilder<A>,
{
    pub fn new(inner: B, target_size: usize) -> Self {
        Self {
            inner_builder: inner,
            bitmap: BitVec::<u8, Lsb0>::with_capacity(target_size),
            target_size,
            _phantom: PhantomData,
        }
    }
}

impl<A, B> BlockBuilder<A> for NullableBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A> + NonNullableBlockBuilder<A>,
{
    fn append(&mut self, item: Option<&A::Item>) {
        match item {
            Some(item) => {
                self.inner_builder.append_value(item);
                self.bitmap.push(true);
            }
            None => {
                self.inner_builder.append_default();
                self.bitmap.push(false);
            }
        }
    }

    fn estimated_size(&self) -> usize {
        let bitmap_byte_len = (self.bitmap.len() + 7) / 8;
        self.inner_builder.estimated_size() + bitmap_byte_len
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        self.inner_builder.get_statistics_with_bitmap(&self.bitmap)
    }

    fn should_finish(&self, next_item: &Option<&A::Item>) -> bool {
        // +1 here since bitmap may extend a byte
        self.inner_builder.should_finish(next_item)
            || !self.inner_builder.is_empty()
                && self.inner_builder.estimated_size_with_next_item(next_item) + 1
                    > self.target_size
    }

    fn finish(self) -> Vec<u8> {
        let mut data = self.inner_builder.finish();
        data.extend(self.bitmap.as_raw_slice().iter());
        data.put_u32_le(self.bitmap.as_raw_slice().len() as u32);
        data
    }

    fn get_target_size(&self) -> usize {
        self.target_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::secondary::block::{PlainBlobBlockBuilder, PlainPrimitiveBlockBuilder};

    #[test]
    fn test_build_i32() {
        let inner_builder = PlainPrimitiveBlockBuilder::<i32>::new(128);
        let mut builder = NullableBlockBuilder::new(inner_builder, 128);

        builder.append(Some(&1));
        builder.append(Some(&2));
        builder.append(Some(&3));
        builder.append(None);

        assert_eq!(builder.estimated_size(), 16 + 1);
        assert!(!builder.should_finish(&Some(&4)));
        builder.finish();
    }

    #[test]
    fn test_build_varchar() {
        let inner_builder = PlainBlobBlockBuilder::<str>::new(128);
        let mut builder = NullableBlockBuilder::new(inner_builder, 128);

        builder.append(Some("233"));
        builder.append(Some("23333"));
        builder.append(Some("2333333"));
        builder.append(None);

        assert_eq!(builder.estimated_size(), 15 + 4 * 4 + 1);
        assert!(!builder.should_finish(&Some("23333333")));
        builder.finish();
    }
}
