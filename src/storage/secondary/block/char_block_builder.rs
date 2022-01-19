// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use super::BlockBuilder;
use crate::array::Utf8Array;

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
        let data = Vec::with_capacity(target_size * char_width as usize);
        Self {
            data,
            char_width: char_width as usize,
            target_size,
        }
    }
}

impl BlockBuilder<Utf8Array> for PlainCharBlockBuilder {
    fn append(&mut self, item: Option<&str>) {
        let item = item
            .expect("nullable item found in non-nullable block builder")
            .as_bytes();
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

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn should_finish(&self, _next_item: &Option<&str>) -> bool {
        !self.data.is_empty() && self.estimated_size() + self.char_width > self.target_size
    }

    fn distinct_count(&self) -> usize {
        let mut distinct_values = HashSet::<&[u8]>::new();
        for item in self.data.chunks(self.char_width) {
            distinct_values.insert(item);
        }
        distinct_values.len()
    }

    fn finish(self) -> Vec<u8> {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_build_char() {
        let mut builder = PlainCharBlockBuilder::new(128, 40);
        let width_40_char = ["2"].iter().cycle().take(40).join("");
        builder.append(Some("233"));
        builder.append(Some("2333"));
        builder.append(Some(&width_40_char));
        assert_eq!(builder.estimated_size(), 120);
        assert!(builder.should_finish(&Some("2333333")));
        builder.finish();
    }
}
