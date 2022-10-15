// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;

use bytes::Buf;

use super::{Block, BlockIterator};
use crate::array::{Array, ArrayBuilder};
use crate::storage::secondary::block::decode_u32;
pub fn decode_rle_block(data: Block) -> (usize, Block, Block) {
    let mut buffer = &data[..];
    let rle_num = buffer.get_u32_le() as usize; // rle_row_count
    let rle_length = std::mem::size_of::<u32>() * 2 + buffer.get_u32_le() as usize;
    let rle_data = data.slice(std::mem::size_of::<u32>() * 2..rle_length);

    let block_data = data.slice(rle_length..);
    (rle_num, rle_data, block_data)
}

/// Scans one or several arrays from the RLE Primitive block content,
/// including plain block and nullable block.
pub struct RleBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
{
    /// Block iterator
    block_iter: B,

    /// rle block
    rle_block: Vec<u32>,

    /// Indicates current position in the rle block
    cur_row: usize,

    /// Indicates how many rows get scanned for cur_row
    cur_scanned_count: usize,

    /// Indicates the number of rows in the rle block
    rle_row_count: usize,

    /// Indicates the element of current row get from block_iter
    cur_element: Option<<A::Item as ToOwned>::Owned>,

    /// Indicates how many rows get scanned for this iterator
    row_scanned_count: usize,

    /// Total count of elements in block
    row_count: usize,

    /// If never_used is true, get an item from child iter in the beginning of next_batch()
    never_used: bool,
}

impl<A, B> RleBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
{
    pub fn new(block_iter: B, rle_block: Block, rle_row_count: usize) -> Self {
        let mut slice = &rle_block[..];
        let rle_block = decode_u32(&mut slice).unwrap();
        let mut row_count: usize = 0;
        for count in &rle_block {
            row_count += *count as usize;
        }

        Self {
            block_iter,
            rle_block,
            cur_row: 0,
            cur_scanned_count: 0,
            rle_row_count,
            cur_element: None,
            row_scanned_count: 0,
            row_count,
            never_used: true,
        }
    }

    fn get_cur_rle_count(&mut self) -> u32 {
        self.rle_block[self.cur_row]
    }

    fn get_next_element(&mut self) -> Option<Option<<A::Item as ToOwned>::Owned>> {
        let mut array_builder = A::Builder::new();
        if self.block_iter.next_batch(Some(1), &mut array_builder) == 0 {
            return None;
        }
        Some(array_builder.finish().get(0).map(|x| x.to_owned()))
    }
}

impl<A, B> BlockIterator<A> for RleBlockIterator<A, B>
where
    A: Array,
    B: BlockIterator<A>,
{
    fn next_batch(&mut self, expected_size: Option<usize>, builder: &mut A::Builder) -> usize {
        if self.cur_row >= self.rle_row_count {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        // If self.never_used is true, then we need to get the first element from block_iter
        // Every time we get only one item from block_iter
        if self.never_used {
            self.never_used = false;
            if let Some(element) = self.get_next_element() {
                self.cur_element = element;
            } else {
                return 0;
            }
        }
        let mut cur_rle_count = self.get_cur_rle_count();

        loop {
            if let Some(expected_size) = expected_size {
                assert!(expected_size > 0);
                if cnt >= expected_size {
                    break;
                }
            }

            // Check if we need to get the next array from block_iter
            if self.cur_scanned_count < cur_rle_count as usize {
                builder.push(self.cur_element.as_ref().map(|x| x.borrow()));
                self.cur_scanned_count += 1;
                self.row_scanned_count += 1;
                cnt += 1;
            } else {
                // Every time cur_row is updated, we need to get the next array from block_iter
                // And reset cur_scanned_count
                self.cur_row += 1;
                self.cur_scanned_count = 0;
                if self.cur_row >= self.rle_row_count {
                    break;
                }
                if let Some(element) = self.get_next_element() {
                    self.cur_element = element;
                } else {
                    break;
                }
                cur_rle_count = self.get_cur_rle_count();
            }
        }

        cnt
    }

    fn skip(&mut self, cnt: usize) {
        let mut cnt = cnt;
        let mut skip_count: usize = 0;
        while cnt > 0 {
            let cur_rle_count = self.get_cur_rle_count();
            let cur_left = cur_rle_count as usize - self.cur_scanned_count;
            if cur_left > cnt {
                self.cur_scanned_count += cnt;
                self.row_scanned_count += cnt;
                break;
            } else {
                cnt -= cur_left;
                self.row_scanned_count += cur_left;
                self.cur_scanned_count = 0;
                self.cur_row += 1;
                skip_count += 1;
                if self.cur_row >= self.rle_row_count {
                    break;
                }
            }
        }
        if skip_count > 0 {
            self.block_iter.skip(skip_count - 1);
            if let Some(element) = self.get_next_element() {
                self.cur_element = element;
            }
        }
    }

    fn remaining_items(&self) -> usize {
        self.row_count - self.row_scanned_count
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use super::super::{PlainCharBlockBuilder, RleBlockBuilder};
    use super::RleBlockIterator;
    use crate::array::{
        ArrayBuilder, ArrayToVecExt, BlobArray, BlobArrayBuilder, I32Array, I32ArrayBuilder,
        Utf8Array, Utf8ArrayBuilder,
    };
    use crate::storage::secondary::block::{
        decode_nullable_block, decode_rle_block, BlockBuilder, NullableBlockBuilder,
        NullableBlockIterator, PlainBlobBlockBuilder, PlainBlobBlockIterator,
        PlainCharBlockIterator, PlainPrimitiveBlockBuilder, PlainPrimitiveBlockIterator,
    };
    use crate::storage::secondary::BlockIterator;
    use crate::types::{Blob, BlobRef};

    #[test]
    fn test_scan_rle_i32() {
        // Test primitive rle block iterator for i32
        let builder = PlainPrimitiveBlockBuilder::new(20);
        let mut rle_builder =
            RleBlockBuilder::<I32Array, PlainPrimitiveBlockBuilder<i32>>::new(builder);
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
        let mut scanner = RleBlockIterator::<I32Array, PlainPrimitiveBlockIterator<i32>>::new(
            block_iter, rle_data, rle_num,
        );

        let mut builder = I32ArrayBuilder::new();

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some(1)]);

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 5);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(builder.finish().to_vec(), vec![Some(2), Some(2)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);

        assert_eq!(builder.finish().to_vec(), vec![Some(3), Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_rle_nullable_i32() {
        // Test primitive nullable rle block iterator for i32
        let inner_builder = PlainPrimitiveBlockBuilder::<i32>::new(50);
        let builder = NullableBlockBuilder::new(inner_builder, 50);
        let mut rle_builder = RleBlockBuilder::new(builder);
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
        let (inner_block, bitmap_block) = decode_nullable_block(block_data);
        let inner_iter = PlainPrimitiveBlockIterator::<i32>::new(inner_block, rle_num);
        let block_iter = NullableBlockIterator::new(inner_iter, bitmap_block);
        let mut scanner = RleBlockIterator::new(block_iter, rle_data, rle_num);

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
        let builder = PlainCharBlockBuilder::new(120, 40);
        let mut rle_builder = RleBlockBuilder::<Utf8Array, PlainCharBlockBuilder>::new(builder);

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
        let mut scanner = RleBlockIterator::<Utf8Array, PlainCharBlockIterator>::new(
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
        let builder = PlainBlobBlockBuilder::new(30);
        let mut rle_builder =
            RleBlockBuilder::<Utf8Array, PlainBlobBlockBuilder<str>>::new(builder);
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
        let mut scanner = RleBlockIterator::<Utf8Array, PlainBlobBlockIterator<str>>::new(
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
        let builder = PlainBlobBlockBuilder::new(30);
        let mut rle_builder =
            RleBlockBuilder::<BlobArray, PlainBlobBlockBuilder<BlobRef>>::new(builder);
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
        let mut scanner = RleBlockIterator::<BlobArray, PlainBlobBlockIterator<BlobRef>>::new(
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

    #[test]
    fn test_scan_rle_skip() {
        // Test primitive nullable rle block iterator for i32
        let inner_builder = PlainPrimitiveBlockBuilder::<i32>::new(50);
        let builder = NullableBlockBuilder::new(inner_builder, 50);
        let mut rle_builder = RleBlockBuilder::new(builder);
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
        let (inner_block, bitmap_block) = decode_nullable_block(block_data);
        let inner_iter = PlainPrimitiveBlockIterator::<i32>::new(inner_block, rle_num);
        let block_iter = NullableBlockIterator::new(inner_iter, bitmap_block);
        let mut scanner = RleBlockIterator::new(block_iter, rle_data, rle_num);

        let mut builder = I32ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 15);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(builder.finish().to_vec(), vec![Some(1), Some(1)]);

        scanner.skip(8);
        assert_eq!(scanner.remaining_items(), 5);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 3);

        assert_eq!(builder.finish().to_vec(), vec![None, None, Some(3)]);

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 1);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
