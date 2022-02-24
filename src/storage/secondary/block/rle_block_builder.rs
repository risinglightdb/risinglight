// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;

use bytes::BufMut;
use risinglight_proto::rowset::BlockStatistics;

use super::BlockBuilder;
use crate::array::Array;

/// Encodes fixed-width data into a block with run-length encoding. The layout is
/// rle counts and data from other block builder
/// ```plain
/// | rle_counts_num (u32) | rle_count (u16) | rle_count | data | data | (may be bit) |
/// ```
pub struct RleBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq,
{
    block_builder: B,
    rle_counts: Vec<u16>,
    previous_value: Option<<A::Item as ToOwned>::Owned>,
    cur_count: u16,
}

impl<A, B> RleBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq,
{
    pub fn new(block_builder: B) -> Self {
        Self {
            block_builder,
            rle_counts: Vec::new(),
            previous_value: None,
            cur_count: 0,
        }
    }
}

impl<A, B> BlockBuilder<A> for RleBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq,
{
    fn append(&mut self, item: Option<&A::Item>) {
        if self.cur_count == 0 {
            // only happens for the very first append
            self.previous_value = item.map(|x| x.to_owned());
            self.block_builder.append(item);
            self.cur_count = 1;
            return;
        }
        if item != self.previous_value.as_ref().map(|x| x.borrow()) || self.cur_count == u16::MAX {
            self.previous_value = item.map(|x| x.to_owned());
            self.block_builder.append(item);
            self.rle_counts.push(self.cur_count);
            self.cur_count = 1;
        } else {
            self.cur_count += 1;
        }
    }

    fn estimated_size(&self) -> usize {
        self.block_builder.estimated_size()
            + self.rle_counts.len() * std::mem::size_of::<u16>()
            + std::mem::size_of::<u32>()
            + (self.cur_count != 0) as usize * std::mem::size_of::<u16>()
    }

    fn should_finish(&self, next_item: &Option<&A::Item>) -> bool {
        self.block_builder.should_finish(next_item)
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        self.block_builder.get_statistics()
    }

    fn finish(mut self) -> Vec<u8> {
        let mut encoded_data: Vec<u8> = vec![];
        if self.cur_count == 0 {
            // No data at all
            return encoded_data;
        }
        self.rle_counts.push(self.cur_count);
        encoded_data.put_u32_le(self.rle_counts.len() as u32);
        for count in self.rle_counts {
            encoded_data.put_u16_le(count);
        }
        let data = self.block_builder.finish();
        encoded_data.extend(data);
        encoded_data
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::super::{
        PlainBlobBlockBuilder, PlainCharBlockBuilder, PlainPrimitiveBlockBuilder,
        PlainPrimitiveNullableBlockBuilder,
    };
    use super::*;
    use crate::array::{I32Array, Utf8Array};

    #[test]
    fn test_build_rle_primitive_i32() {
        // Test primitive rle block builder for i32
        let builder = PlainPrimitiveBlockBuilder::new(14);
        let mut rle_builder =
            RleBlockBuilder::<I32Array, PlainPrimitiveBlockBuilder<i32>>::new(builder);
        for item in [Some(&1)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 4 * 3 + 2 * 3 + 4);
        assert!(rle_builder.should_finish(&Some(&3)));
        assert!(rle_builder.should_finish(&Some(&4)));
        rle_builder.finish();
    }

    #[test]
    fn test_build_rle_primitive_nullable_i32() {
        // Test primitive nullable rle block builder for i32
        let builder = PlainPrimitiveNullableBlockBuilder::new(48);
        let mut rle_builder =
            RleBlockBuilder::<I32Array, PlainPrimitiveNullableBlockBuilder<i32>>::new(builder);
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&1)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&4)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&5)]
            .iter()
            .cycle()
            .cloned()
            .take(u16::MAX as usize * 2)
        {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 11 * 4 + 2 + 11 * 2 + 4);
        assert!(rle_builder.should_finish(&Some(&5)));
        rle_builder.finish();
    }

    #[test]
    fn test_build_rle_char() {
        // Test rle block builder for char
        let builder = PlainCharBlockBuilder::new(120, 40);
        let mut rle_builder = RleBlockBuilder::<Utf8Array, PlainCharBlockBuilder>::new(builder);

        let width_40_char = ["2"].iter().cycle().take(40).join("");

        for item in [Some("233")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some("2333")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 40 * 3 + 2 * 3 + 4);
        assert!(rle_builder.should_finish(&Some(&width_40_char[..])));
        // should_finish is not very accurate
        assert!(rle_builder.should_finish(&Some("2333333")));
        rle_builder.finish();
    }

    #[test]
    fn test_build_rle_varchar() {
        // Test rle block builder for varchar
        let builder = PlainBlobBlockBuilder::new(30);
        let mut rle_builder =
            RleBlockBuilder::<Utf8Array, PlainBlobBlockBuilder<str>>::new(builder);
        for item in [Some("233")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some("23333")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some("2333333")].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 15 + 4 * 3 + 2 * 3 + 4); // 37
        assert!(rle_builder.should_finish(&Some("2333333")));
        // should_finish is not very accurate
        assert!(rle_builder.should_finish(&Some("23333333")));
        rle_builder.finish();
    }
}
