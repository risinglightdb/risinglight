// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use bitvec::prelude::{BitVec, Lsb0};
use risinglight_proto::rowset::BlockStatistics;

use super::super::encode::PrimitiveFixedWidthEncode;
use super::super::statistics::StatisticsBuilder;
use super::{BlockBuilder, NonNullableBlockBuilder};
use crate::array::Array;

/// Encodes fixed-width data into a block. The layout is simply an array of
/// little endian fixed-width data.
pub struct PlainPrimitiveBlockBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,
    target_size: usize,
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainPrimitiveBlockBuilder<T> {
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        Self {
            data,
            target_size,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> NonNullableBlockBuilder<T::ArrayType>
    for PlainPrimitiveBlockBuilder<T>
{
    fn append_value(&mut self, item: &<T::ArrayType as Array>::Item) {
        item.encode(&mut self.data);
    }

    fn append_default(&mut self) {
        T::DEFAULT_VALUE.encode(&mut self.data)
    }

    fn get_statistics_with_bitmap(&self, selection: &BitVec<u8, Lsb0>) -> Vec<BlockStatistics> {
        let selection_empty = selection.is_empty();
        let mut stats_builder = StatisticsBuilder::new();
        for (idx, item) in self.data.chunks(T::WIDTH).enumerate() {
            if selection_empty || selection[idx] {
                stats_builder.add_item(Some(item));
            }
        }
        stats_builder.get_statistics()
    }

    fn estimated_size_with_next_item(
        &self,
        _next_item: &Option<&<T::ArrayType as Array>::Item>,
    ) -> usize {
        self.estimated_size() + T::WIDTH
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockBuilder<T::ArrayType> for PlainPrimitiveBlockBuilder<T> {
    fn append(&mut self, item: Option<&T>) {
        match item {
            Some(item) => self.append_value(item),
            None => self.append_default(),
        }
    }

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn should_finish(&self, _next_item: &Option<&T>) -> bool {
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
    use super::*;

    #[test]
    fn test_build_i32() {
        let mut builder = PlainPrimitiveBlockBuilder::<i32>::new(128);
        builder.append(Some(&1));
        builder.append(Some(&2));
        builder.append_value(&3);
        builder.append_default();
        assert_eq!(builder.estimated_size(), 16);
        assert!(!builder.should_finish(&Some(&4)));
        builder.finish();
    }
}
