// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bitvec::prelude::{BitVec, Lsb0};
use risinglight_proto::rowset::BlockStatistics;

use super::super::statistics::StatisticsBuilder;
use super::{BlockBuilder, NonNullableBlockBuilder};
use crate::array::{Array, Utf8Array};

/// Encodes fixed-width char into a block.
///
/// Every record is composed of a item width and the actual data. For example,
///
/// ```plain
/// | length (1B, u8) | data (10B) |
/// ```
pub struct PlainCharBlockBuilder {
    data: Vec<u8>,
    char_width: usize,
    target_size: usize,
}

impl PlainCharBlockBuilder {
    pub fn new(target_size: usize, char_width: u64) -> Self {
        let data = Vec::with_capacity(target_size);
        Self {
            data,
            char_width: char_width as usize,
            target_size,
        }
    }
}

impl NonNullableBlockBuilder<Utf8Array> for PlainCharBlockBuilder {
    fn append_value(&mut self, item: &<Utf8Array as Array>::Item) {
        let item = item.as_bytes();
        if item.len() > self.char_width {
            panic!(
                "item length {} > char width {}",
                item.len(),
                self.char_width
            );
        }
        self.data.extend(item);
        self.data.extend(
            [0].iter()
                .cycle()
                .take(self.char_width - item.len())
                .cloned(),
        );
    }

    fn append_default(&mut self) {
        self.data
            .extend([0].iter().cycle().take(self.char_width).cloned());
    }

    fn get_statistics_with_bitmap(&self, selection: &BitVec<u8, Lsb0>) -> Vec<BlockStatistics> {
        let selection_empty = selection.is_empty();
        let mut stats_builder = StatisticsBuilder::new();
        for (idx, item) in self.data.chunks(self.char_width).enumerate() {
            if selection_empty || selection[idx] {
                stats_builder.add_item(Some(item));
            }
        }
        stats_builder.get_statistics()
    }

    fn estimated_size_with_next_item(
        &self,
        _next_item: &Option<&<Utf8Array as Array>::Item>,
    ) -> usize {
        self.estimated_size() + self.char_width
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl BlockBuilder<Utf8Array> for PlainCharBlockBuilder {
    fn append(&mut self, item: Option<&str>) {
        match item {
            Some(item) => self.append_value(item),
            None => self.append_default(),
        }
    }

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn should_finish(&self, _next_item: &Option<&str>) -> bool {
        !self.is_empty() && self.estimated_size_with_next_item(_next_item) > self.target_size
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        self.get_statistics_with_bitmap(&BitVec::new())
    }

    fn finish(self) -> Vec<u8> {
        self.data
    }

    fn get_target_size(&self) -> usize {
        self.target_size
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_build_char() {
        let mut builder = PlainCharBlockBuilder::new(168, 40);
        let width_40_char = ["2"].iter().cycle().take(40).join("");
        builder.append(Some("233"));
        builder.append(Some("2333"));
        builder.append_value(&width_40_char);
        builder.append_default();
        assert_eq!(builder.estimated_size(), 160);
        assert!(builder.should_finish(&Some("2333333")));
        builder.finish();
    }
}
