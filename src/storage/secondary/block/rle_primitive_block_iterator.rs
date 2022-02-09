// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use bitvec::prelude::Lsb0;
use bitvec::slice::BitSlice;
use bytes::Buf;

use super::super::PrimitiveFixedWidthEncode;
use super::{Block, BlockIterator};
use crate::array::{Array, ArrayBuilder};

/// Scans one or several arrays from the RLE Primitive block content,
/// including plain block and nullable block.
pub struct RLEPrimitiveBlockIterator<T: PrimitiveFixedWidthEncode> {
    /// Block content
    block: Block,

    /// Whether nullable
    nullable: bool,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    /// current pos count
    cur_count: usize,

    /// The number of rle_counts
    rle_counts_num: usize,

    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> RLEPrimitiveBlockIterator<T> {
    pub fn new(block: Block, nullable: bool) -> Self {
        let mut rle_counts_num = &block[..];
        let rle_counts_num = rle_counts_num.get_u32_le() as usize;
        Self {
            block,
            nullable,
            next_row: 0,
            cur_count: 0,
            rle_counts_num,
            _phantom: PhantomData,
        }
    }
}

const U16_LEN: usize = std::mem::size_of::<u16>();
const U32_LEN: usize = std::mem::size_of::<u32>();

impl<T: PrimitiveFixedWidthEncode> BlockIterator<T::ArrayType> for RLEPrimitiveBlockIterator<T> {
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
        let rle_counts_length = U32_LEN + U16_LEN * self.rle_counts_num;
        let rle_counts_buffer = &self.block[U32_LEN..rle_counts_length];
        let data_buffer = &self.block[rle_counts_length..];

        // Or check the length of the block to confirm whether it is nullable?
        let bitmap_slice: Option<&BitSlice<u8>> = if self.nullable {
            let bitmap_buffer = &data_buffer[T::WIDTH * self.rle_counts_num..];
            Some(BitSlice::<u8, Lsb0>::from_slice(bitmap_buffer))
        } else {
            None
        };

        let mut cur_data_buf = &data_buffer[self.next_row * T::WIDTH..];
        let mut data = T::decode(&mut cur_data_buf);
        let mut cur_rle_counts_buf = &rle_counts_buffer[self.next_row * U16_LEN..];
        let mut rle_count = cur_rle_counts_buf.get_u16_le();

        loop {
            if let Some(expected_size) = expected_size {
                assert!(expected_size > 0);
                if cnt >= expected_size {
                    break;
                }
            }

            if self.cur_count < rle_count as usize {
                if let Some(bitmap_slice) = bitmap_slice {
                    if bitmap_slice[self.next_row] {
                        builder.push(Some(&data));
                    } else {
                        builder.push(None);
                    }
                } else {
                    builder.push(Some(&data));
                }
                self.cur_count += 1;
                cnt += 1;
            } else {
                self.next_row += 1;
                self.cur_count = 0;
                if self.next_row >= self.rle_counts_num {
                    break;
                }
                cur_data_buf = &data_buffer[self.next_row * T::WIDTH..];
                data = T::decode(&mut cur_data_buf);
                cur_rle_counts_buf = &rle_counts_buffer[self.next_row * U16_LEN..];
                rle_count = cur_rle_counts_buf.get_u16_le();
            }
        }

        cnt
    }

    fn skip(&mut self, cnt: usize) {
        let mut cnt = cnt;
        let rle_counts_buffer = &self.block[U32_LEN..];
        while cnt > 0 {
            let mut cur_rle_counts_buf = &rle_counts_buffer[self.next_row * U16_LEN..];
            let rle_count = cur_rle_counts_buf.get_u16_le();
            let cur_left = rle_count as usize - self.cur_count;
            if cur_left > cnt {
                self.cur_count += cnt;
                cnt = 0;
            } else {
                cnt -= cur_left;
                self.cur_count = 0;
                self.next_row += 1;
                if self.next_row >= self.rle_counts_num {
                    break;
                }
            }
        }
    }

    fn remaining_items(&self) -> usize {
        let mut remaining_items: usize = 0;
        let rle_counts_buffer = &self.block[U32_LEN..];
        for next_row in self.next_row..self.rle_counts_num {
            let mut cur_rle_counts_buf = &rle_counts_buffer[next_row * U16_LEN..];
            let rle_count = cur_rle_counts_buf.get_u16_le();
            remaining_items += rle_count as usize;
        }
        remaining_items - self.cur_count
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::RLEPrimitiveBlockIterator;
    use crate::array::{ArrayBuilder, ArrayToVecExt, I32ArrayBuilder};
    use crate::storage::secondary::block::{
        BlockBuilder, PlainPrimitiveBlockBuilder, PlainPrimitiveNullableBlockBuilder,
        RLEPrimitiveBlockBuilder,
    };
    use crate::storage::secondary::BlockIterator;

    #[test]
    fn test_scan_rle_i32() {
        // Test primitive rle block builder for i32
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

        let mut scanner = RLEPrimitiveBlockIterator::<i32>::new(Bytes::from(data), false);

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
        // Test primitive nullable rle block builder for i32
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

        let mut scanner = RLEPrimitiveBlockIterator::<i32>::new(Bytes::from(data), true);

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
