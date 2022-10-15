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
    nested_builder: B,
    bitmap: BitVec<u8, Lsb0>,
    target_size: usize,
    _phantom: PhantomData<A>,
}

impl<A, B> NullableBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A> + NonNullableBlockBuilder<A>,
{
    // TODO: replace PlainPrimitiveBlockBuilder
    #[allow(dead_code)]
    pub fn new(nested: B, target_size: usize) -> Self {
        Self {
            nested_builder: nested,
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
                self.nested_builder.append_value(item);
                self.bitmap.push(true);
            }
            None => {
                self.nested_builder.append_default();
                self.bitmap.push(false);
            }
        }
    }

    fn estimated_size(&self) -> usize {
        let bitmap_byte_len = (self.bitmap.len() + 7) / 8;
        self.nested_builder.estimated_size() + bitmap_byte_len
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        self.nested_builder.get_statistics_with_bitmap(&self.bitmap)
    }

    fn should_finish(&self, next_item: &Option<&A::Item>) -> bool {
        self.nested_builder.should_finish(next_item)
    }

    fn finish(self) -> Vec<u8> {
        let mut data = self.nested_builder.finish();
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
