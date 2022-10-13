// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use bytes::Buf;

use super::{Block, BlockIterator, NonNullableBlockIterator};
use crate::array::{Array, ArrayBuilder};
use crate::storage::secondary::encode::BlobEncode;

/// Scans one or several arrays from the block content.
pub struct PlainBlobBlockIterator<T: BlobEncode + ?Sized> {
    /// Block content
    block: Block,

    /// Total count of elements in block
    row_count: usize,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    phantom: PhantomData<T>,
}

impl<T: BlobEncode + ?Sized> PlainBlobBlockIterator<T> {
    pub fn new(block: Block, row_count: usize) -> Self {
        Self {
            block,
            row_count,
            next_row: 0,
            phantom: PhantomData,
        }
    }
}

impl<T: BlobEncode + ?Sized> NonNullableBlockIterator<T::ArrayType> for PlainBlobBlockIterator<T> {
    fn next_batch_non_null(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <T::ArrayType as Array>::Builder,
    ) -> usize {
        if self.next_row >= self.row_count {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        const OFFSET: usize = std::mem::size_of::<u32>();
        let offsets_length = OFFSET * self.row_count;
        let offset_buffer = &self.block[0..offsets_length];
        let data_buffer = &self.block[offsets_length..];

        loop {
            if let Some(expected_size) = expected_size {
                assert!(expected_size > 0);
                if cnt >= expected_size {
                    break;
                }
            }

            if self.next_row >= self.row_count {
                break;
            }

            let from;
            let to;

            if self.next_row == 0 {
                let mut cur_offsets = offset_buffer;
                from = 0;
                to = cur_offsets.get_u32_le() as usize;
            } else {
                let mut cur_offsets = &offset_buffer[(self.next_row - 1) * OFFSET..];
                from = cur_offsets.get_u32_le() as usize;
                to = cur_offsets.get_u32_le() as usize;
            }
            builder.push(Some(T::from_byte_slice(&data_buffer[from..to])));

            cnt += 1;
            self.next_row += 1;
        }

        cnt
    }
}

impl<T: BlobEncode + ?Sized> BlockIterator<T::ArrayType> for PlainBlobBlockIterator<T> {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <T::ArrayType as Array>::Builder,
    ) -> usize {
        self.next_batch_non_null(expected_size, builder)
    }

    fn skip(&mut self, cnt: usize) {
        self.next_row += cnt;
    }

    fn remaining_items(&self) -> usize {
        self.row_count - self.next_row
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::array::{ArrayBuilder, ArrayToVecExt, BlobArrayBuilder, Utf8ArrayBuilder};
    use crate::storage::secondary::block::{BlockBuilder, PlainBlobBlockBuilder};
    use crate::storage::secondary::BlockIterator;
    use crate::types::{Blob, BlobRef};

    #[test]
    fn test_scan_blob() {
        let mut builder = PlainBlobBlockBuilder::<BlobRef>::new(128);
        let input = vec![
            Some(BlobRef::new("233".as_bytes())),
            Some(BlobRef::new("2333".as_bytes())),
            Some(BlobRef::new("23333".as_bytes())),
        ];

        input.iter().for_each(|v| builder.append(*v));
        let data = builder.finish();

        let mut scanner = PlainBlobBlockIterator::<BlobRef>::new(Bytes::from(data), 3);

        let mut builder = BlobArrayBuilder::new();

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 2);

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(Blob::from("2333".as_bytes()))]
        );

        let mut builder = BlobArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 1);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(Blob::from("23333".as_bytes()))]
        );

        let mut builder = BlobArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_varchar() {
        let mut builder = PlainBlobBlockBuilder::<str>::new(128);
        builder.append(Some("233"));
        builder.append(Some("2333"));
        builder.append(Some("23333"));
        let data = builder.finish();

        let mut scanner = PlainBlobBlockIterator::<str>::new(Bytes::from(data), 3);

        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 2);

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some("2333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some("23333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
