// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bytes::Buf;

use super::super::PrimitiveFixedWidthEncode;
use super::{Block, BlockIterator};
use crate::array::{Array, ArrayBuilder};
use crate::types::NativeType;

/// Scans one or several arrays from the RLE Primitive block content,
/// including plain block and nullable block.
pub struct RLEPrimitiveBlockIterator<T, B>
where
    T: PrimitiveFixedWidthEncode + NativeType,
    B: BlockIterator<T::ArrayType>,
{
    /// Block iterator
    block_iter: B,

    /// rle block
    rle_block: Block,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    /// current pos count
    cur_count: usize,

    /// The number of rle_counts
    rle_counts_num: usize,

    /// Current array
    cur_array: Option<T::ArrayType>,
}

impl<T, B> RLEPrimitiveBlockIterator<T, B>
where
    T: PrimitiveFixedWidthEncode + NativeType,
    B: BlockIterator<T::ArrayType>,
{
    pub fn new(block_iter: B, rle_block: Block, rle_counts_num: usize) -> Self {
        Self {
            block_iter,
            rle_block,
            next_row: 0,
            cur_count: 0,
            rle_counts_num,
            cur_array: None,
        }
    }

    fn get_cur_rle_count(&self) -> u16 {
        let mut rle_buffer = &self.rle_block[self.next_row * std::mem::size_of::<u16>()..];
        let rle_count = rle_buffer.get_u16_le();
        rle_count
    }
}

impl<T, B> BlockIterator<T::ArrayType> for RLEPrimitiveBlockIterator<T, B>
where
    T: PrimitiveFixedWidthEncode + NativeType,
    B: BlockIterator<T::ArrayType>,
{
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <T::ArrayType as Array>::Builder,
    ) -> usize {
        if self.next_row >= self.rle_counts_num {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        if self.cur_array.is_none() {
            let mut array_builder = <T::ArrayType as Array>::Builder::new();
            self.block_iter.next_batch(Some(1), &mut array_builder);
            self.cur_array = Some(array_builder.finish());
        }
        let mut rle_count = self.get_cur_rle_count();

        loop {
            if let Some(expected_size) = expected_size {
                assert!(expected_size > 0);
                if cnt >= expected_size {
                    break;
                }
            }

            if self.cur_count < rle_count as usize {
                builder.append(self.cur_array.as_ref().unwrap());
                self.cur_count += 1;
                cnt += 1;
            } else {
                self.next_row += 1;
                self.cur_count = 0;
                if self.next_row >= self.rle_counts_num {
                    break;
                }
                let mut array_builder = <T::ArrayType as Array>::Builder::new();
                self.block_iter.next_batch(Some(1), &mut array_builder);
                self.cur_array = Some(array_builder.finish());
                rle_count = self.get_cur_rle_count();
            }
        }

        cnt
    }

    fn skip(&mut self, cnt: usize) {
        let mut cnt = cnt;
        while cnt > 0 {
            let rle_count = self.get_cur_rle_count();
            let cur_left = rle_count as usize - self.cur_count;
            if cur_left > cnt {
                self.cur_count += cnt;
                cnt = 0;
            } else {
                cnt -= cur_left;
                self.cur_count = 0;
                self.next_row += 1;
                self.block_iter.skip(1);
                if self.next_row >= self.rle_counts_num {
                    break;
                }
            }
        }
    }

    fn remaining_items(&self) -> usize {
        let mut remaining_items: usize = 0;
        for next_row in self.next_row..self.rle_counts_num {
            let mut rle_buffer = &self.rle_block[next_row * std::mem::size_of::<u16>()..];
            let rle_count = rle_buffer.get_u16_le();
            remaining_items += rle_count as usize;
        }
        remaining_items - self.cur_count
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes};

    use super::RLEPrimitiveBlockIterator;
    use crate::array::{ArrayBuilder, ArrayToVecExt, I32ArrayBuilder};
    use crate::storage::secondary::block::{
        BlockBuilder, PlainPrimitiveBlockBuilder, PlainPrimitiveBlockIterator,
        PlainPrimitiveNullableBlockBuilder, PlainPrimitiveNullableBlockIterator,
        RLEPrimitiveBlockBuilder,
    };
    use crate::storage::secondary::BlockIterator;

    #[test]
    fn test_scan_rle_i32() {
        // Test primitive rle block iterator for i32
        let builder = PlainPrimitiveBlockBuilder::new(20);
        let mut rle_builder =
            RLEPrimitiveBlockBuilder::<i32, PlainPrimitiveBlockBuilder<i32>>::new(builder, 20);
        for item in [Some(&1)].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        let data = rle_builder.finish();

        let mut buffer = &data[..];
        let rle_counts_num = buffer.get_u32_le() as usize;
        let rle_counts_length =
            std::mem::size_of::<u32>() + std::mem::size_of::<u16>() * rle_counts_num;
        let rle_data = data[std::mem::size_of::<u32>()..rle_counts_length].to_vec();
        let block_data = data[rle_counts_length..].to_vec();
        let block_iter =
            PlainPrimitiveBlockIterator::<i32>::new(Bytes::from(block_data), rle_counts_num);
        let mut scanner = RLEPrimitiveBlockIterator::<i32, PlainPrimitiveBlockIterator<i32>>::new(
            block_iter,
            Bytes::from(rle_data),
            rle_counts_num,
        );

        let mut builder = I32ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 6);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(builder.finish().to_vec(), vec![Some(2), Some(2)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);

        assert_eq!(builder.finish().to_vec(), vec![Some(2), Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 2);

        assert_eq!(builder.finish().to_vec(), vec![Some(3), Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_rle_nullable_i32() {
        // Test primitive nullable rle block iterator for i32
        let builder = PlainPrimitiveNullableBlockBuilder::new(70);
        let mut rle_builder = RLEPrimitiveBlockBuilder::<
            i32,
            PlainPrimitiveNullableBlockBuilder<i32>,
        >::new(builder, 70);
        for item in [None].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some(&1)].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        let data = rle_builder.finish();

        let mut buffer = &data[..];
        let rle_counts_num = buffer.get_u32_le() as usize;
        let rle_counts_length =
            std::mem::size_of::<u32>() + std::mem::size_of::<u16>() * rle_counts_num;
        let rle_data = data[std::mem::size_of::<u32>()..rle_counts_length].to_vec();
        let block_data = data[rle_counts_length..].to_vec();
        let block_iter = PlainPrimitiveNullableBlockIterator::<i32>::new(
            Bytes::from(block_data),
            rle_counts_num,
        );
        let mut scanner =
            RLEPrimitiveBlockIterator::<i32, PlainPrimitiveNullableBlockIterator<i32>>::new(
                block_iter,
                Bytes::from(rle_data),
                rle_counts_num,
            );

        let mut builder = I32ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 15);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(builder.finish().to_vec(), vec![Some(1), Some(1)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(6), &mut builder), 6);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(1), None, None, None, Some(2), Some(2)]
        );

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(8), &mut builder), 7);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(2), None, None, None, Some(3), Some(3), Some(3)]
        );

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
