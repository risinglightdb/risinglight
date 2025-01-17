// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use bytes::Buf;

use super::{Block, BlockIterator, NonNullableBlockIterator};
use crate::array::{ArrayBuilder, VectorArray, VectorArrayBuilder};
use crate::types::{VectorRef, F64};

/// Scans one or several arrays from the block content.
pub struct PlainVectorBlockIterator {
    /// Block content
    block: Block,

    /// Total count of elements in block
    row_count: usize,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    /// Fixed-size buffer for vector data
    vec_buffer: Vec<F64>,

    /// The size for each vector element
    element_size: usize,
}

impl PlainVectorBlockIterator {
    pub fn new(block: Block, row_count: usize) -> Self {
        let element_size =
            (&block[block.len() - std::mem::size_of::<u32>()..block.len()]).get_u32() as usize;

        Self {
            block,
            row_count,
            next_row: 0,
            vec_buffer: Vec::new(),
            element_size,
        }
    }
}

impl NonNullableBlockIterator<VectorArray> for PlainVectorBlockIterator {
    fn next_batch_non_null(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut VectorArrayBuilder,
    ) -> usize {
        if self.next_row >= self.row_count {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        let data_buffer = &self.block[..];

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

            let from = self.next_row * self.element_size * std::mem::size_of::<f64>();
            let to = from + self.element_size * std::mem::size_of::<f64>();
            assert!((to - from) % std::mem::size_of::<f64>() == 0);
            self.vec_buffer.clear();
            self.vec_buffer
                .reserve(self.element_size * std::mem::size_of::<f64>());
            let mut buf = &data_buffer[from..to];
            for _ in 0..self.element_size {
                self.vec_buffer.push(F64::from(buf.get_f64_le()));
            }
            builder.push(Some(VectorRef::new(&self.vec_buffer)));

            cnt += 1;
            self.next_row += 1;
        }

        cnt
    }
}

impl BlockIterator<VectorArray> for PlainVectorBlockIterator {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut VectorArrayBuilder,
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
    use crate::array::{ArrayBuilder, ArrayToVecExt, VectorArrayBuilder};
    use crate::storage::secondary::block::{BlockBuilder, PlainVectorBlockBuilder};
    use crate::storage::secondary::BlockIterator;
    use crate::types::Vector;

    #[test]
    fn test_scan_vector() {
        let mut builder = PlainVectorBlockBuilder::new(128);
        let input = [
            Some(Vector::new(vec![1.0, 2.0, 3.0])),
            Some(Vector::new(vec![4.0, 5.0, 6.0])),
            Some(Vector::new(vec![7.0, 8.0, 9.0])),
        ];

        input
            .iter()
            .for_each(|v| builder.append(v.as_ref().map(|v| v.as_ref())));
        let data = builder.finish();

        let mut scanner = PlainVectorBlockIterator::new(Bytes::from(data), 3);

        let mut builder = VectorArrayBuilder::new();

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 2);

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(
                VectorRef::new(&[F64::from(4.0), F64::from(5.0), F64::from(6.0)]).to_vector()
            )]
        );

        let mut builder = VectorArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 1);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(
                VectorRef::new(&[F64::from(7.0), F64::from(8.0), F64::from(9.0)]).to_vector()
            )]
        );

        let mut builder = VectorArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
