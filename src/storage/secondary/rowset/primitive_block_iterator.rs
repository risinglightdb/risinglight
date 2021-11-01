use std::marker::PhantomData;

use crate::array::{Array, ArrayBuilder};

use super::{Block, BlockIterator, PrimitiveFixedWidthEncode};

/// Scans one or several arrays from the block content.
pub struct PlainPrimitiveBlockIterator<T: PrimitiveFixedWidthEncode> {
    /// Block content
    block: Block,

    /// Total count of elements in block
    length: usize,

    /// Indicates if the current block iterator has finished scanning
    finished: bool,

    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainPrimitiveBlockIterator<T> {
    pub fn new(block: Block, length: usize) -> Self {
        Self {
            block,
            length,
            finished: false,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockIterator<T::ArrayType> for PlainPrimitiveBlockIterator<T> {
    fn next_batch(&mut self) -> Option<T::ArrayType> {
        if self.finished {
            return None;
        }
        // Currently, the `BlockIterator` on primitive blocks simply yields all data at once.
        self.finished = true;

        // TODO(chi): error handling on corrupted block

        let mut builder = <T::ArrayType as Array>::Builder::new(self.length);
        let mut remaining_bytes = &self.block[..];
        for _ in 0..self.length {
            builder.push(Some(&T::decode(&mut remaining_bytes)));
        }
        Some(builder.finish())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::array::ArrayToVecExt;
    use crate::storage::secondary::rowset::BlockBuilder;
    use crate::storage::secondary::rowset::PlainPrimitiveBlockBuilder;
    use crate::storage::secondary::BlockIterator;

    use super::PlainPrimitiveBlockIterator;

    #[test]
    fn test_scan_i32() {
        let mut builder = PlainPrimitiveBlockBuilder::<i32>::new(128);
        builder.append(Some(&1));
        builder.append(Some(&2));
        builder.append(Some(&3));
        let data = builder.finish();

        let mut scanner = PlainPrimitiveBlockIterator::<i32>::new(Bytes::from(data), 3);

        assert_eq!(
            scanner.next_batch().unwrap().to_vec(),
            vec![Some(1), Some(2), Some(3)]
        );

        assert_eq!(scanner.next_batch(), None);
    }
}
