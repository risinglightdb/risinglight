// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use super::super::PrimitiveFixedWidthEncode;
use super::{Block, BlockIterator, NonNullableBlockIterator};
use crate::array::{Array, ArrayBuilder};

/// Scans one or several arrays from the block content.
pub struct PlainPrimitiveBlockIterator<T: PrimitiveFixedWidthEncode> {
    /// Block content
    block: Block,

    /// Total count of elements in block
    row_count: usize,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainPrimitiveBlockIterator<T> {
    pub fn new(block: Block, row_count: usize) -> Self {
        Self {
            block,
            row_count,
            next_row: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> NonNullableBlockIterator<T::ArrayType>
    for PlainPrimitiveBlockIterator<T>
{
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
        let mut buffer = &self.block[self.next_row * T::WIDTH..];

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

            builder.push(Some(&T::decode(&mut buffer)));
            cnt += 1;
            self.next_row += 1;
        }

        cnt
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockIterator<T::ArrayType> for PlainPrimitiveBlockIterator<T> {
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

    use super::PlainPrimitiveBlockIterator;
    use crate::array::{ArrayBuilder, ArrayToVecExt, I32ArrayBuilder};
    use crate::storage::secondary::block::{BlockBuilder, PlainPrimitiveBlockBuilder};
    use crate::storage::secondary::BlockIterator;

    #[test]
    fn test_scan_i32() {
        let mut builder = PlainPrimitiveBlockBuilder::<i32>::new(128);
        builder.append(Some(&1));
        builder.append(Some(&2));
        builder.append(Some(&3));
        let data = builder.finish();

        let mut scanner = PlainPrimitiveBlockIterator::<i32>::new(Bytes::from(data), 3);

        let mut builder = I32ArrayBuilder::new();

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 2);

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some(2)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some(3)]);

        let mut builder = I32ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
