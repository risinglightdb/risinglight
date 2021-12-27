use std::marker::PhantomData;

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;
use rust_decimal::Decimal;

use super::super::{
    Block, BlockIterator, PlainPrimitiveBlockIterator, PlainPrimitiveNullableBlockIterator,
    PrimitiveFixedWidthEncode,
};
use super::{BlockIteratorFactory, ConcreteColumnIterator};
use crate::array::Array;
use crate::types::Date;

/// All supported block iterators for primitive types.
pub enum PrimitiveBlockIteratorImpl<T: PrimitiveFixedWidthEncode> {
    Plain(PlainPrimitiveBlockIterator<T>),
    PlainNullable(PlainPrimitiveNullableBlockIterator<T>),
}

impl<T: PrimitiveFixedWidthEncode> BlockIterator<T::ArrayType> for PrimitiveBlockIteratorImpl<T> {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <T::ArrayType as Array>::Builder,
    ) -> usize {
        match self {
            Self::Plain(it) => it.next_batch(expected_size, builder),
            Self::PlainNullable(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::Plain(it) => it.skip(cnt),
            Self::PlainNullable(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::Plain(it) => it.remaining_items(),
            Self::PlainNullable(it) => it.remaining_items(),
        }
    }
}

pub struct PrimitiveBlockIteratorFactory<T: PrimitiveFixedWidthEncode> {
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PrimitiveBlockIteratorFactory<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

/// Column iterators on primitive types
pub type PrimitiveColumnIterator<T> = ConcreteColumnIterator<
    <T as PrimitiveFixedWidthEncode>::ArrayType,
    PrimitiveBlockIteratorFactory<T>,
>;

pub type I32ColumnIterator = PrimitiveColumnIterator<i32>;
pub type F64ColumnIterator = PrimitiveColumnIterator<f64>;
pub type BoolColumnIterator = PrimitiveColumnIterator<bool>;
pub type DecimalColumnIterator = PrimitiveColumnIterator<Decimal>;
pub type DateColumnIterator = PrimitiveColumnIterator<Date>;

impl<T: PrimitiveFixedWidthEncode> BlockIteratorFactory<T::ArrayType>
    for PrimitiveBlockIteratorFactory<T>
{
    type BlockIteratorImpl = PrimitiveBlockIteratorImpl<T>;

    fn get_iterator_for(
        &self,
        block_type: BlockType,
        block: Block,
        index: &BlockIndex,
        start_pos: usize,
    ) -> Self::BlockIteratorImpl {
        let mut it = match block_type {
            BlockType::Plain => {
                let it = PlainPrimitiveBlockIterator::new(block, index.row_count as usize);
                PrimitiveBlockIteratorImpl::Plain(it)
            }
            BlockType::PlainNullable => {
                let it = PlainPrimitiveNullableBlockIterator::new(block, index.row_count as usize);
                PrimitiveBlockIteratorImpl::PlainNullable(it)
            }
            _ => todo!(),
        };
        it.skip(start_pos - index.first_rowid as usize);
        it
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::array::ArrayToVecExt;
    use crate::storage::secondary::rowset::tests::helper_build_rowset;
    use crate::storage::secondary::{ColumnIterator, PrimitiveColumnIterator};

    #[tokio::test]
    async fn test_scan_i32() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir, false).await;
        let column = rowset.column(0);
        let mut scanner = PrimitiveColumnIterator::<i32>::new(
            column.clone(),
            0,
            PrimitiveBlockIteratorFactory::new(),
        )
        .await;
        let mut recv_data = vec![];
        while let Some((_, data)) = scanner.next_batch(None).await {
            recv_data.extend(data.to_vec());
        }

        for i in 0..100 {
            assert_eq!(
                recv_data[i * 1000..(i + 1) * 1000],
                [1, 2, 3]
                    .iter()
                    .cycle()
                    .cloned()
                    .take(1000)
                    .map(Some)
                    .collect_vec()
            );
        }

        let mut scanner = PrimitiveColumnIterator::<i32>::new(
            column,
            10000,
            PrimitiveBlockIteratorFactory::new(),
        )
        .await;
        for i in 0..10 {
            let (id, data) = scanner.next_batch(Some(1000)).await.unwrap();
            assert_eq!(id, 10000 + i * 1000);
            let left = data.to_vec();
            let right = [1, 2, 3]
                .iter()
                .cycle()
                .cloned()
                .take(1000)
                .map(Some)
                .collect_vec();
            assert_eq!(left.len(), right.len());
            assert_eq!(left, right);
        }
    }
}
