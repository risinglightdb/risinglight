use std::marker::PhantomData;

use bytes::Buf;

use crate::array::{Array, ArrayBuilder};

use super::super::PrimitiveFixedWidthEncode;
use super::{Block, BlockIterator};

/// Scans one or several arrays from the block content.
pub struct PlainPrimitiveNullableBlockIterator<T: PrimitiveFixedWidthEncode> {
    /// Block content
    block: Block,

    /// Total count of elements in block
    row_count: usize,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainPrimitiveNullableBlockIterator<T> {
    pub fn new(block: Block, row_count: usize) -> Self {
        Self {
            block,
            row_count,
            next_row: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockIterator<T::ArrayType>
    for PlainPrimitiveNullableBlockIterator<T>
{
    fn next_batch(
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
        let mut bitmap_buffer = &self.block[self.row_count * T::WIDTH + self.next_row..];

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

            let bitmap = bitmap_buffer.get_u8();
            let data = &T::decode(&mut buffer);

            if bitmap == 0 {
                builder.push(None);
            } else {
                builder.push(Some(data));
            }

            cnt += 1;
            self.next_row += 1;
        }

        cnt
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

    use crate::array::ArrayToVecExt;
    use crate::array::{ArrayBuilder, I32ArrayBuilder};
    use crate::storage::secondary::block::{BlockBuilder, PlainPrimitiveNullableBlockBuilder};
    use crate::storage::secondary::BlockIterator;

    use super::PlainPrimitiveNullableBlockIterator;

    #[test]
    fn test_scan_i32() {
        let mut builder = PlainPrimitiveNullableBlockBuilder::<i32>::new(128);
        builder.append(Some(&1));
        builder.append(None);
        builder.append(Some(&3));
        let data = builder.finish();

        let mut scanner = PlainPrimitiveNullableBlockIterator::<i32>::new(Bytes::from(data), 3);

        let mut builder = I32ArrayBuilder::new(0);

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 2);

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![None]);

        let mut builder = I32ArrayBuilder::new(0);
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some(3)]);

        let mut builder = I32ArrayBuilder::new(0);
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
