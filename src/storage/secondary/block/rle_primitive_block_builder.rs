// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bytes::BufMut;
use risinglight_proto::rowset::BlockStatistics;

use super::super::encode::PrimitiveFixedWidthEncode;
use super::BlockBuilder;

/// Encodes fixed-width data into a block with run-length encoding. The layout is
/// rle counts and data from other block builder
/// ```plain
/// | rle_counts_num (u32) | rle_count (u16) | rle_count | data | data | (may be bit) |
/// ```
pub struct RLEPrimitiveBlockBuilder<T, B>
where
    T: PrimitiveFixedWidthEncode,
    B: BlockBuilder<T::ArrayType>,
{
    block_builder: B,
    rle_counts: Vec<u16>,
    previous_value: Option<T>,
    target_size: usize,
}

impl<T, B> RLEPrimitiveBlockBuilder<T, B>
where
    T: PrimitiveFixedWidthEncode,
    B: BlockBuilder<T::ArrayType>,
{
    pub fn new(block_builder: B, target_size: usize) -> Self {
        Self {
            block_builder,
            rle_counts: Vec::new(),
            previous_value: None,
            target_size,
        }
    }

    fn append_inner(&mut self, item: Option<&T>) {
        self.previous_value = item.map(|item| *item);
        self.block_builder.append(item);
        self.rle_counts.push(1);
    }
}

impl<T, B> BlockBuilder<T::ArrayType> for RLEPrimitiveBlockBuilder<T, B>
where
    T: PrimitiveFixedWidthEncode + PartialEq,
    B: BlockBuilder<T::ArrayType>,
{
    fn append(&mut self, item: Option<&T>) {
        let len = self.rle_counts.len();
        if let Some(item) = item {
            if let Some(previous_value) = &self.previous_value {
                if previous_value == item && self.rle_counts[len - 1] < u16::MAX {
                    self.rle_counts[len - 1] += 1;
                    return;
                }
            }
        } else if self.previous_value.is_none() && len > 0 && self.rle_counts[len - 1] < u16::MAX {
            self.rle_counts[len - 1] += 1;
            return;
        }
        self.append_inner(item);
    }

    fn estimated_size(&self) -> usize {
        self.block_builder.estimated_size() + self.rle_counts.len() * 2
    }

    fn should_finish(&self, next_item: &Option<&T>) -> bool {
        if let &Some(item) = next_item {
            if let Some(previous_value) = &self.previous_value {
                if item == previous_value && self.rle_counts.last().unwrap_or(&0) < &u16::MAX {
                    return false;
                }
            }
        } else if self.previous_value.is_none() && self.rle_counts.last().unwrap_or(&0) < &u16::MAX
        {
            return false;
        }
        self.estimated_size() + T::WIDTH + 2 > self.target_size
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        self.block_builder.get_statistics()
    }

    fn finish(self) -> Vec<u8> {
        let mut encoded_data: Vec<u8> = vec![];
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
    use super::super::{PlainPrimitiveBlockBuilder, PlainPrimitiveNullableBlockBuilder};
    use super::*;

    #[test]
    fn test_build_primitive_rle_i32() {
        // Test primitive rle block builder for i32
        let builder = PlainPrimitiveBlockBuilder::new(20);
        let mut rle_builder =
            RLEPrimitiveBlockBuilder::<i32, PlainPrimitiveBlockBuilder<i32>>::new(builder, 20);
        for item in [Some(&1)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(30) {
            rle_builder.append(item);
        }
        assert_eq!(rle_builder.estimated_size(), 18);
        assert!(!rle_builder.should_finish(&Some(&3)));
        assert!(rle_builder.should_finish(&Some(&4)));
        rle_builder.finish();
    }

    #[test]
    fn test_build_nullable_primitive_rle_i32() {
        // Test primitive nullable rle block builder for i32
        let builder = PlainPrimitiveNullableBlockBuilder::new(70);
        let mut rle_builder = RLEPrimitiveBlockBuilder::<
            i32,
            PlainPrimitiveNullableBlockBuilder<i32>,
        >::new(builder, 70);
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
        // 9 + 2 = 11 items in PlainPrimitiveNullableBlockBuilder = 44 + 2 = 46
        // 11 * 2 for rle_counts, sums result 46 + 22 = 68
        assert_eq!(rle_builder.estimated_size(), 68);
        assert!(rle_builder.should_finish(&Some(&5)));
        rle_builder.finish();
    }
}
