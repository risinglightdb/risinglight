// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use bytes::BufMut;

use super::BlockBuilder;
use crate::array::Utf8Array;

/// Encodes offset and data into a block. The data layout is
/// ```plain
/// | offset (u32) | offset | offset | data | data | data |
/// ```
pub struct PlainVarcharBlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u32>,
    target_size: usize,
}

impl PlainVarcharBlockBuilder {
    #[allow(dead_code)]
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        Self {
            data,
            offsets: vec![],
            target_size,
        }
    }
}

impl BlockBuilder<Utf8Array> for PlainVarcharBlockBuilder {
    fn append(&mut self, item: Option<&str>) {
        let item = item.expect("nullable item found in non-nullable block builder");
        self.data.extend(item.as_bytes());
        self.offsets.push(self.data.len() as u32);
    }

    fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * std::mem::size_of::<u32>()
    }

    fn should_finish(&self, next_item: &Option<&str>) -> bool {
        !self.data.is_empty()
            && self.estimated_size()
                + next_item.map(|x| x.len()).unwrap_or(0)
                + std::mem::size_of::<u32>()
                > self.target_size
    }

    fn distinct_count(&self) -> usize {
        let mut distinct_values = HashSet::<&[u8]>::new();
        let mut last_pos: usize = 0;
        let mut cur_pos;
        for pos in &self.offsets {
            cur_pos = *pos as usize;
            distinct_values.insert(&self.data[last_pos..cur_pos]);
            last_pos = cur_pos;
        }
        distinct_values.len()
    }

    fn finish(self) -> Vec<u8> {
        let mut encoded_data = vec![];
        for offset in self.offsets {
            encoded_data.put_u32_le(offset);
        }
        encoded_data.extend(self.data);
        encoded_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_str() {
        let mut builder = PlainVarcharBlockBuilder::new(128);
        builder.append(Some("233"));
        builder.append(Some("23333"));
        builder.append(Some("2333333"));
        assert_eq!(builder.estimated_size(), 15 + 4 * 3);
        assert!(!builder.should_finish(&Some("23333333")));
        builder.finish();
    }
}
