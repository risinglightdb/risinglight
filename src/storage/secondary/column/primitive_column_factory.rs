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
use crate::storage::secondary::block::FakeBlockIterator;
use crate::types::{Date, Interval};

/// All supported block iterators for primitive types.
pub enum PrimitiveBlockIteratorImpl<T: PrimitiveFixedWidthEncode> {
    Plain(PlainPrimitiveBlockIterator<T>),
    PlainNullable(PlainPrimitiveNullableBlockIterator<T>),
    Fake(FakeBlockIterator<T::ArrayType>),
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
            Self::Fake(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::Plain(it) => it.skip(cnt),
            Self::PlainNullable(it) => it.skip(cnt),
            Self::Fake(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::Plain(it) => it.remaining_items(),
            Self::PlainNullable(it) => it.remaining_items(),
            Self::Fake(it) => it.remaining_items(),
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
pub type IntervalColumnIterator = PrimitiveColumnIterator<Interval>;

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

    fn get_fake_iterator(&self, index: &BlockIndex, start_pos: usize) -> Self::BlockIteratorImpl {
        let mut it =
            PrimitiveBlockIteratorImpl::Fake(FakeBlockIterator::new(index.row_count as usize));
        it.skip(start_pos - index.first_rowid as usize);
        it
    }
}

#[cfg(test)]
mod tests {
    use bitvec::bitvec;
    use bitvec::prelude::BitVec;
    use itertools::Itertools;

    use super::*;
    use crate::array::ArrayToVecExt;
    use crate::storage::secondary::column::Column;
    use crate::storage::secondary::rowset::tests::helper_build_rowset;
    use crate::storage::secondary::{ColumnIterator, PrimitiveColumnIterator};

    #[tokio::test]
    async fn test_scan_i32() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir, false, 1000).await;
        let column = rowset.column(0);
        let mut scanner = PrimitiveColumnIterator::<i32>::new(
            column.clone(),
            0,
            PrimitiveBlockIteratorFactory::new(),
        )
        .await
        .unwrap();
        let mut recv_data = vec![];
        while let Some((_, data)) = scanner.next_batch(None, None).await.unwrap() {
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
        .await
        .unwrap();
        for i in 0..10 {
            let (id, data) = scanner.next_batch(Some(1000), None).await.unwrap().unwrap();
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

    #[tokio::test]
    async fn test_scan_i32_with_filter() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir, false, 1020).await;
        let column = rowset.column(0);

        let none_array = vec![None; 1020];
        let value_array = [1, 2, 3]
            .iter()
            .cycle()
            .cloned()
            .take(1020)
            .map(Some)
            .collect_vec();

        test_i32_0(column.clone(), &none_array).await;
        test_i32_1(column.clone(), &value_array).await;
        test_i32_0_1_0(column.clone(), &none_array, &value_array).await;
        test_i32_with_expected_size(column.clone(), &none_array, &value_array).await;
        test_i32_mix(column.clone(), &none_array, &value_array).await;
    }

    async fn get_data(
        column: Column,
        mut filter_bitmap: BitVec,
        expected_size: Option<usize>,
    ) -> Vec<Option<i32>> {
        let mut scanner = PrimitiveColumnIterator::<i32>::new(
            column.clone(),
            0,
            PrimitiveBlockIteratorFactory::new(),
        )
        .await
        .unwrap();
        let mut recv_data = vec![];

        while let Some((start_row_id, data)) = scanner
            .next_batch(expected_size, Some(&filter_bitmap))
            .await
            .unwrap()
        {
            recv_data.extend(data.to_vec());
            filter_bitmap =
                filter_bitmap.split_off((scanner.get_current_row_id() - start_row_id) as usize);
        }
        recv_data
    }

    async fn test_i32_0(column: Column, none_array: &[Option<i32>]) {
        let filter_bitmap = bitvec![0; 100 * 1020];

        let recv_data = get_data(column, filter_bitmap, None).await;

        for i in 1..100 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *none_array);
        }
    }

    async fn test_i32_1(column: Column, value_array: &[Option<i32>]) {
        let filter_bitmap = bitvec![1; 100 * 1020];

        let recv_data = get_data(column, filter_bitmap, None).await;

        for i in 0..100 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *value_array);
        }
    }

    async fn test_i32_0_1_0(
        column: Column,
        none_array: &[Option<i32>],
        value_array: &[Option<i32>],
    ) {
        let mut left_bitmap = bitvec![0; 50 * 1020];
        let mut middle_bitmap = bitvec![1; 25 * 1020];
        let mut right_bitmap = bitvec![0; 25 * 1020];
        middle_bitmap.append(&mut right_bitmap);
        left_bitmap.append(&mut middle_bitmap);

        let recv_data = get_data(column, left_bitmap, None).await;

        for i in 1..50 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *none_array);
        }
        for i in 50..75 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *value_array);
        }
        for i in 75..100 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *none_array);
        }
    }

    async fn test_i32_with_expected_size(
        column: Column,
        none_array: &[Option<i32>],
        value_array: &[Option<i32>],
    ) {
        let mut left_bitmap = bitvec![0; 50 * 1020];
        let mut middle_bitmap = bitvec![1; 25 * 1020];
        let mut right_bitmap = bitvec![0; 25 * 1020];
        middle_bitmap.append(&mut right_bitmap);
        left_bitmap.append(&mut middle_bitmap);

        let recv_data = get_data(column, left_bitmap, Some(789)).await;

        for i in 1..50 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *none_array);
        }
        for i in 50..75 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *value_array);
        }
        for i in 75..100 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *none_array);
        }
    }

    async fn test_i32_mix(column: Column, none_array: &[Option<i32>], value_array: &[Option<i32>]) {
        let mut left_bitmap = bitvec![0; 50 * 1020];
        let mut middle_bitmap = bitvec![1; 25 * 1020];
        let mut right_bitmap = bitvec![0; 25 * 1020];
        middle_bitmap.append(&mut right_bitmap);
        left_bitmap.append(&mut middle_bitmap);

        // begin, middle and end have been tested
        for i in 75..100 {
            left_bitmap.set(i * 1020 + 1019, true);
        }

        let recv_data = get_data(column, left_bitmap, Some(1200)).await;

        for i in 1..50 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *none_array);
        }
        for i in 50..75 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *value_array);
        }
        for i in 75..100 {
            assert_eq!(recv_data[i * 1020..(i + 1) * 1020], *value_array);
        }
    }

    #[tokio::test]
    async fn test_skip() {
        let len = 1020;
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir, false, len).await;
        let column = rowset.column(0);

        skip_helper(column.clone(), len / 2, len).await;
        skip_helper(column.clone(), len, len).await;
        skip_helper(column.clone(), len + len / 2, len).await;
        skip_helper(column.clone(), len * 2, len).await;
    }

    async fn skip_helper(column: Column, cnt: usize, len: usize) {
        let mut scanner = PrimitiveColumnIterator::<i32>::new(
            column.clone(),
            0,
            PrimitiveBlockIteratorFactory::new(),
        )
        .await
        .unwrap();
        let mut recv_data = vec![];
        let bitmap_len = if cnt % len == 0 { len } else { cnt % len };
        let filter_bitmap = bitvec![1; bitmap_len];

        scanner.skip(cnt);
        if let Some((start_row_id, data)) = scanner
            .next_batch(None, Some(&filter_bitmap))
            .await
            .unwrap()
        {
            recv_data.extend(data.to_vec());
            assert_eq!(start_row_id as usize, cnt);
        }

        let mut value_array = [1, 2, 3]
            .iter()
            .cycle()
            .cloned()
            .take(len)
            .map(Some)
            .collect_vec();
        value_array = value_array.split_off(cnt % len);
        assert_eq!(recv_data, value_array);
    }
}
