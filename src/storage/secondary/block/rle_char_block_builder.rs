// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bytes::BufMut;
use risinglight_proto::rowset::BlockStatistics;

use super::BlockBuilder;
use crate::array::Utf8Array;

/// Encodes fixed-width char or offset and data into a block with run-length encoding.
/// The layout is rle counts and data from other block builder
/// ```plain
/// | rle_counts_num (u32) | rle_count (u16) | rle_count | data | data |
/// ```
pub struct RLECharBlockBuilder<B: BlockBuilder<Utf8Array>> {
    block_builder: B,
    rle_counts: Vec<u16>,
    previous_value: Option<String>,
    target_size: usize,
    char_width: Option<u64>,
}

impl<B: BlockBuilder<Utf8Array>> RLECharBlockBuilder<B> {
    pub fn new(block_builder: B, target_size: usize, char_width: Option<u64>) -> Self {
        Self {
            block_builder,
            rle_counts: Vec::new(),
            previous_value: None,
            target_size,
            char_width,
        }
    }

    fn append_inner(&mut self, item: Option<&str>) {
        self.previous_value = item.map(String::from);
        self.block_builder.append(item);
        self.rle_counts.push(1);
    }
}

impl<B> BlockBuilder<Utf8Array> for RLECharBlockBuilder<B>
where
    B: BlockBuilder<Utf8Array>,
{
    fn append(&mut self, item: Option<&str>) {
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
        self.block_builder.estimated_size()
            + self.rle_counts.len() * std::mem::size_of::<u16>()
            + std::mem::size_of::<u32>()
    }

    fn should_finish(&self, next_item: &Option<&str>) -> bool {
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
        if let Some(char_width) = self.char_width {
            !self.rle_counts.is_empty()
                && self.estimated_size() + char_width as usize + std::mem::size_of::<u16>()
                    > self.target_size
        } else {
            !self.rle_counts.is_empty()
                && self.estimated_size()
                    + next_item.map(|x| x.len()).unwrap_or(0)
                    + std::mem::size_of::<u32>()
                    + std::mem::size_of::<u16>()
                    > self.target_size
        }
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
    use itertools::Itertools;

    use super::super::{PlainCharBlockBuilder, PlainVarcharBlockBuilder};
    use super::*;

    #[test]
    fn test_build_rle_char() {
        // Test rle block builder for char
        let builder = PlainCharBlockBuilder::new(150, 40);
        let mut rle_builder =
            RLECharBlockBuilder::<PlainCharBlockBuilder>::new(builder, 150, Some(40));

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
        assert!(!rle_builder.should_finish(&Some(&width_40_char[..])));
        assert!(rle_builder.should_finish(&Some("2333333")));
        rle_builder.finish();
    }

    #[test]
    fn test_build_rle_varchar() {
        // Test rle block builder for varchar
        let builder = PlainVarcharBlockBuilder::new(40);
        let mut rle_builder =
            RLECharBlockBuilder::<PlainVarcharBlockBuilder>::new(builder, 40, None);
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
        assert!(rle_builder.should_finish(&Some("23333333")));
        assert!(!rle_builder.should_finish(&Some("2333333")));
        rle_builder.finish();
    }
}
