// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use bitvec::prelude::{BitVec, Lsb0};
use itertools::enumerate;
use risinglight_proto::rowset::BlockStatistics;

use super::super::statistics::StatisticsBuilder;
use super::super::PrimitiveFixedWidthEncode;
use super::BlockBuilder;

/// Encodes fixed-width data into a block, with null element support.
///
/// The layout is fixed-width data and a u8 bitmap, concatenated together.
pub struct PlainPrimitiveNullableBlockBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,
    bitmap: BitVec<u8, Lsb0>,
    target_size: usize,
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainPrimitiveNullableBlockBuilder<T> {
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        let bitmap = BitVec::<u8, Lsb0>::with_capacity(target_size);
        Self {
            data,
            target_size,
            bitmap,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockBuilder<T::ArrayType>
    for PlainPrimitiveNullableBlockBuilder<T>
{
    fn append(&mut self, item: Option<&T>) {
        if let Some(item) = item {
            item.encode(&mut self.data);
            self.bitmap.push(true);
        } else {
            T::DEFAULT_VALUE.encode(&mut self.data);
            self.bitmap.push(false);
        }
    }

    fn estimated_size(&self) -> usize {
        let bitmap_byte_len = (self.bitmap.len() + 7) / 8;
        self.data.len() + bitmap_byte_len
    }

    fn should_finish(&self, _next_item: &Option<&T>) -> bool {
        !self.data.is_empty() && self.estimated_size() + 1 + T::WIDTH > self.target_size
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        let mut stats_builder = StatisticsBuilder::new();
        for (idx, item) in enumerate(self.data.chunks(T::WIDTH)) {
            if self.bitmap[idx] {
                stats_builder.add_item(Some(item));
            }
        }
        stats_builder.get_statistics()
    }

    fn finish(self) -> Vec<u8> {
        let mut data = self.data;
        data.extend(self.bitmap.as_raw_slice().iter());
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_i32() {
        let mut builder = PlainPrimitiveNullableBlockBuilder::<i32>::new(128);
        builder.append(Some(&1));
        builder.append(None);
        builder.append(Some(&3));
        builder.append(Some(&4));
        assert_eq!(builder.estimated_size(), 17);
        assert!(!builder.should_finish(&Some(&5)));
        let data = builder.finish();
        // bitmap should be 1011 and Lsb0, so u8 will be 0b1101 = 13
        let expected_data: Vec<u8> = vec![1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 13];
        assert_eq!(data, expected_data);
    }
}
