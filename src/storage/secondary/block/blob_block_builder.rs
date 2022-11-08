// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use bitvec::prelude::{BitVec, Lsb0};
use bytes::BufMut;
use risinglight_proto::rowset::BlockStatistics;

use super::super::statistics::StatisticsBuilder;
use super::{BlockBuilder, NonNullableBlockBuilder};
use crate::array::Array;
use crate::storage::secondary::encode::BlobEncode;

/// Encodes offset and data into a block. The data layout is
/// ```plain
/// | offset (u32) | offset | offset | data | data | data |
/// ```
pub struct PlainBlobBlockBuilder<T: BlobEncode + ?Sized> {
    data: Vec<u8>,
    offsets: Vec<u32>,
    target_size: usize,

    phantom: PhantomData<T>,
}

impl<T: BlobEncode + ?Sized> PlainBlobBlockBuilder<T> {
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        Self {
            data,
            offsets: vec![],
            target_size,
            phantom: PhantomData,
        }
    }
}

impl<T: BlobEncode + ?Sized> NonNullableBlockBuilder<T::ArrayType> for PlainBlobBlockBuilder<T> {
    fn append_value(&mut self, item: &<T::ArrayType as Array>::Item) {
        self.data.extend(item.to_byte_slice());
        self.offsets.push(self.data.len() as u32);
    }

    fn append_default(&mut self) {
        // don't extend `self.data` since empty value is default
        self.offsets.push(self.data.len() as u32);
    }

    fn get_statistics_with_bitmap(&self, selection: &BitVec<u8, Lsb0>) -> Vec<BlockStatistics> {
        let selection_empty = selection.is_empty();
        let mut stats_builder = StatisticsBuilder::new();
        let mut last_pos: usize = 0;
        let mut cur_pos;
        for (idx, pos) in self.offsets.iter().enumerate() {
            cur_pos = *pos as usize;
            if selection_empty || selection[idx] {
                stats_builder.add_item(Some(&self.data[last_pos..cur_pos]));
            }
            last_pos = cur_pos;
        }
        stats_builder.get_statistics()
    }

    fn estimated_size_with_next_item(
        &self,
        next_item: &Option<&<T::ArrayType as Array>::Item>,
    ) -> usize {
        self.estimated_size() + next_item.map(|x| x.len()).unwrap_or(0) + std::mem::size_of::<u32>()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T: BlobEncode + ?Sized> BlockBuilder<T::ArrayType> for PlainBlobBlockBuilder<T> {
    fn append(&mut self, item: Option<&T>) {
        match item {
            Some(item) => {
                self.append_value(item);
            }
            None => {
                self.append_default();
            }
        }
    }

    fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * std::mem::size_of::<u32>()
    }

    fn should_finish(&self, next_item: &Option<&T>) -> bool {
        !self.is_empty() && self.estimated_size_with_next_item(next_item) > self.target_size
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        self.get_statistics_with_bitmap(&BitVec::new())
    }

    fn finish(self) -> Vec<u8> {
        let mut encoded_data = vec![];
        for offset in self.offsets {
            encoded_data.put_u32_le(offset);
        }
        encoded_data.extend(self.data);
        encoded_data
    }

    fn get_target_size(&self) -> usize {
        self.target_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_str() {
        let mut builder = PlainBlobBlockBuilder::<str>::new(128);
        builder.append(Some("233"));
        builder.append(Some("23333"));
        builder.append_value("2333333");
        builder.append_default();
        assert_eq!(builder.estimated_size(), 15 + 4 * 4);
        assert!(!builder.should_finish(&Some("23333333")));
        builder.finish();
    }
}
