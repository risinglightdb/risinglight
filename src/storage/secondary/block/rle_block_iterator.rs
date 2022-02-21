// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bytes::Buf;

use super::{Block, BlockIterator};
use crate::array::{Array, ArrayBuilder};

/// Scans one or several arrays from the RLE Primitive block content,
/// including plain block and nullable block.
pub struct RLEBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
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
    cur_array: Option<A>,
}

impl<A, B> RLEBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
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
        rle_buffer.get_u16_le()
    }
}

impl<A, B> BlockIterator<A> for RLEBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
{
    fn next_batch(&mut self, expected_size: Option<usize>, builder: &mut A::Builder) -> usize {
        if self.next_row >= self.rle_counts_num {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        if self.cur_array.is_none() {
            let mut array_builder = A::Builder::new();
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
                let mut array_builder = A::Builder::new();
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
    use bytes::Bytes;
    use itertools::Itertools;

    use super::super::{PlainCharBlockBuilder, RLEBlockBuilder};
    use super::RLEBlockIterator;
    use crate::array::{
        ArrayBuilder, ArrayToVecExt, BlobArray, BlobArrayBuilder, I32Array, I32ArrayBuilder,
        Utf8Array, Utf8ArrayBuilder,
    };
    use crate::storage::secondary::block::{
        decode_rle_block, BlockBuilder, PlainBlobBlockBuilder, PlainBlobBlockIterator,
        PlainCharBlockIterator, PlainPrimitiveBlockBuilder, PlainPrimitiveBlockIterator,
        PlainPrimitiveNullableBlockBuilder, PlainPrimitiveNullableBlockIterator,
    };
    use crate::storage::secondary::BlockIterator;
    use crate::types::{Blob, BlobRef};

    #[test]
    fn test_scan_rle_i32() {
        // Test primitive rle block iterator for i32
        let builder = PlainPrimitiveBlockBuilder::new(0);
        let mut rle_builder =
            RLEBlockBuilder::<I32Array, PlainPrimitiveBlockBuilder<i32>>::new(builder, 20);
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

        let (rle_num, rle_data, block_data) = decode_rle_block(Bytes::from(data));
        let block_iter = PlainPrimitiveBlockIterator::new(block_data, rle_num);
        let mut scanner = RLEBlockIterator::<I32Array, PlainPrimitiveBlockIterator<i32>>::new(
            block_iter, rle_data, rle_num,
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
        let builder = PlainPrimitiveNullableBlockBuilder::new(0);
        let mut rle_builder =
            RLEBlockBuilder::<I32Array, PlainPrimitiveNullableBlockBuilder<i32>>::new(builder, 70);
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

        let (rle_num, rle_data, block_data) = decode_rle_block(Bytes::from(data));
        let block_iter = PlainPrimitiveNullableBlockIterator::new(block_data, rle_num);
        let mut scanner =
            RLEBlockIterator::<I32Array, PlainPrimitiveNullableBlockIterator<i32>>::new(
                block_iter, rle_data, rle_num,
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

    #[test]
    fn test_scan_rle_char() {
        let builder = PlainCharBlockBuilder::new(0, 40);
        let mut rle_builder =
            RLEBlockBuilder::<Utf8Array, PlainCharBlockBuilder>::new(builder, 150);

        let width_40_char = ["2"].iter().cycle().take(40).join("");

        for item in [Some("233")].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some("2333")].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        let data = rle_builder.finish();

        let (rle_num, rle_data, block_data) = decode_rle_block(Bytes::from(data));
        let block_iter = PlainCharBlockIterator::new(block_data, rle_num, 40);
        let mut scanner = RLEBlockIterator::<Utf8Array, PlainCharBlockIterator>::new(
            block_iter, rle_data, rle_num,
        );

        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 6);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("2333".to_string()), Some("2333".to_string())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("2333".to_string()), Some(width_40_char.clone())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 2);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(width_40_char.clone()), Some(width_40_char.clone())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_rle_varchar() {
        // Test rle block iterator for varchar
        let builder = PlainBlobBlockBuilder::new(0);
        let mut rle_builder =
            RLEBlockBuilder::<Utf8Array, PlainBlobBlockBuilder<str>>::new(builder, 40);
        for item in [Some("233")].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some("23333")].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some("2333333")].iter().cycle().cloned().take(2) {
            rle_builder.append(item);
        }
        let data = rle_builder.finish();

        let (rle_num, rle_data, block_data) = decode_rle_block(Bytes::from(data));
        let block_iter = PlainBlobBlockIterator::new(block_data, rle_num);
        let mut scanner = RLEBlockIterator::<Utf8Array, PlainBlobBlockIterator<str>>::new(
            block_iter, rle_data, rle_num,
        );

        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 5);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("23333".to_string()), Some("23333".to_string())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(6), &mut builder), 3);

        assert_eq!(
            builder.finish().to_vec(),
            vec![
                Some("23333".to_string()),
                Some("2333333".to_string()),
                Some("2333333".to_string())
            ]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_rle_blob() {
        // Test rle block iterator for blob
        let builder = PlainBlobBlockBuilder::new(0);
        let mut rle_builder =
            RLEBlockBuilder::<BlobArray, PlainBlobBlockBuilder<BlobRef>>::new(builder, 40);
        for item in [Some(BlobRef::new("233".as_bytes()))]
            .iter()
            .cycle()
            .cloned()
            .take(3)
        {
            rle_builder.append(item);
        }
        for item in [Some(BlobRef::new("23333".as_bytes()))]
            .iter()
            .cycle()
            .cloned()
            .take(3)
        {
            rle_builder.append(item);
        }
        for item in [Some(BlobRef::new("2333333".as_bytes()))]
            .iter()
            .cycle()
            .cloned()
            .take(2)
        {
            rle_builder.append(item);
        }
        let data = rle_builder.finish();

        let (rle_num, rle_data, block_data) = decode_rle_block(Bytes::from(data));
        let block_iter = PlainBlobBlockIterator::new(block_data, rle_num);
        let mut scanner = RLEBlockIterator::<BlobArray, PlainBlobBlockIterator<BlobRef>>::new(
            block_iter, rle_data, rle_num,
        );

        let mut builder = BlobArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 5);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(
            builder.finish().to_vec(),
            vec![
                Some(Blob::from("23333".as_bytes())),
                Some(Blob::from("23333".as_bytes()))
            ]
        );

        let mut builder = BlobArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(6), &mut builder), 3);

        assert_eq!(
            builder.finish().to_vec(),
            vec![
                Some(Blob::from("23333".as_bytes())),
                Some(Blob::from("2333333".as_bytes())),
                Some(Blob::from("2333333".as_bytes()))
            ]
        );

        let mut builder = BlobArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
