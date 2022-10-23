// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;
use rust_decimal::Decimal;

use super::super::{Block, BlockIterator, PlainPrimitiveBlockIterator, PrimitiveFixedWidthEncode};
use super::{BlockIteratorFactory, ConcreteColumnIterator};
use crate::array::Array;
use crate::storage::secondary::block::{
    decode_nullable_block, decode_rle_block, FakeBlockIterator, NullableBlockIterator,
    RleBlockIterator,
};
use crate::types::{Date, Interval, F64};

/// All supported block iterators for primitive types.
pub enum PrimitiveBlockIteratorImpl<T: PrimitiveFixedWidthEncode> {
    Plain(PlainPrimitiveBlockIterator<T>),
    PlainNullable(NullableBlockIterator<T::ArrayType, PlainPrimitiveBlockIterator<T>>),
    RunLength(RleBlockIterator<T::ArrayType, PlainPrimitiveBlockIterator<T>>),
    RleNullable(
        RleBlockIterator<
            T::ArrayType,
            NullableBlockIterator<T::ArrayType, PlainPrimitiveBlockIterator<T>>,
        >,
    ),
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
            Self::RunLength(it) => it.next_batch(expected_size, builder),
            Self::RleNullable(it) => it.next_batch(expected_size, builder),
            Self::Fake(it) => it.next_batch(expected_size, builder),
        }
    }

    fn skip(&mut self, cnt: usize) {
        match self {
            Self::Plain(it) => it.skip(cnt),
            Self::PlainNullable(it) => it.skip(cnt),
            Self::RunLength(it) => it.skip(cnt),
            Self::RleNullable(it) => it.skip(cnt),
            Self::Fake(it) => it.skip(cnt),
        }
    }

    fn remaining_items(&self) -> usize {
        match self {
            Self::Plain(it) => it.remaining_items(),
            Self::PlainNullable(it) => it.remaining_items(),
            Self::RunLength(it) => it.remaining_items(),
            Self::RleNullable(it) => it.remaining_items(),
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
pub type I64ColumnIterator = PrimitiveColumnIterator<i64>;
pub type F64ColumnIterator = PrimitiveColumnIterator<F64>;
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
                let (inner_block, bitmap_block) = decode_nullable_block(block);
                let inner_it =
                    PlainPrimitiveBlockIterator::new(inner_block, index.row_count as usize);
                let it = NullableBlockIterator::new(inner_it, bitmap_block);
                PrimitiveBlockIteratorImpl::PlainNullable(it)
            }
            BlockType::RunLength => {
                let (rle_num, rle_data, block_data) = decode_rle_block(block);
                let block_iter = PlainPrimitiveBlockIterator::<T>::new(block_data, rle_num);
                let it = RleBlockIterator::<T::ArrayType, PlainPrimitiveBlockIterator<T>>::new(
                    block_iter, rle_data, rle_num,
                );
                PrimitiveBlockIteratorImpl::RunLength(it)
            }
            BlockType::RleNullable => {
                let (rle_num, rle_data, block_data) = decode_rle_block(block);
                let (inner_block, bitmap_block) = decode_nullable_block(block_data);
                let inner_it =
                    PlainPrimitiveBlockIterator::<T>::new(inner_block, index.row_count as usize);
                let block_iter = NullableBlockIterator::new(inner_it, bitmap_block);
                let it = RleBlockIterator::<
                    T::ArrayType,
                    NullableBlockIterator<T::ArrayType, PlainPrimitiveBlockIterator<T>>,
                >::new(block_iter, rle_data, rle_num);
                PrimitiveBlockIteratorImpl::RleNullable(it)
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
    use itertools::Itertools;

    use super::*;
    use crate::array::ArrayToVecExt;
    use crate::storage::secondary::column::Column;
    use crate::storage::secondary::rowset::tests::{helper_build_rle_rowset, helper_build_rowset};
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
        while let Some((_, data)) = scanner.next_batch(None).await.unwrap() {
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
            let (id, data) = scanner.next_batch(Some(1000)).await.unwrap().unwrap();
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
    async fn test_scan_rle_i32() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rle_rowset(&tempdir, false, 1000).await;
        let column = rowset.column(0);
        let mut scanner = PrimitiveColumnIterator::<i32>::new(
            column.clone(),
            0,
            PrimitiveBlockIteratorFactory::new(),
        )
        .await
        .unwrap();
        let mut recv_data = vec![];
        while let Some((_, data)) = scanner.next_batch(None).await.unwrap() {
            recv_data.extend(data.to_vec());
        }

        for i in 0..100 {
            assert_eq!(
                recv_data[i * 1000..(i + 1) * 1000],
                [1, 1, 2, 2, 2]
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
            let (id, data) = scanner.next_batch(Some(1000)).await.unwrap().unwrap();
            assert_eq!(id, 10000 + i * 1000);
            let left = data.to_vec();
            let right = [1, 1, 2, 2, 2]
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
        let size = if cnt % len == 0 { len } else { cnt % len };

        scanner.skip(cnt);
        if let Some((start_row_id, data)) = scanner.next_batch(Some(size)).await.unwrap() {
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

    #[tokio::test]
    async fn test_skip_multiple_times() {
        let len = 1020;
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir, false, len).await;
        let column = rowset.column(0);

        let mut scanner = PrimitiveColumnIterator::<i32>::new(
            column.clone(),
            0,
            PrimitiveBlockIteratorFactory::new(),
        )
        .await
        .unwrap();
        let mut recv_data = vec![];
        let size = len;

        // not specify `expected_size`, not aligned
        scanner.skip(size);
        scanner.skip(size / 2);
        if let Some((start_row_id, data)) = scanner.next_batch(None).await.unwrap() {
            recv_data.extend(data.to_vec());
            assert_eq!(start_row_id as usize, size + size / 2);
        }

        let value_array = [1, 2, 3]
            .iter()
            .cycle()
            .cloned()
            .take(len / 2)
            .map(Some)
            .collect_vec();
        assert_eq!(recv_data, value_array);

        // specify `expected_size`, not aligned
        recv_data = vec![];
        scanner.skip(size / 2);

        if let Some((start_row_id, data)) = scanner.next_batch(Some(len)).await.unwrap() {
            recv_data.extend(data.to_vec());
            assert_eq!(start_row_id as usize, size * 2 + size / 2);
        }

        let value_array = [1, 2, 3]
            .iter()
            .cycle()
            .cloned()
            .take(len)
            .map(Some)
            .collect_vec();
        assert_eq!(recv_data, value_array);

        // specify `expected_size`, aligned
        recv_data = vec![];
        scanner.skip(size + size / 2);

        if let Some((start_row_id, data)) = scanner.next_batch(Some(len)).await.unwrap() {
            recv_data.extend(data.to_vec());
            assert_eq!(start_row_id as usize, size * 5);
        }

        let value_array = [1, 2, 3]
            .iter()
            .cycle()
            .cloned()
            .take(len)
            .map(Some)
            .collect_vec();
        assert_eq!(recv_data, value_array);

        scanner.skip(size / 2);
        scanner.skip(size);
        scanner.skip(size);

        if let Some((start_row_id, _)) = scanner.next_batch(Some(len)).await.unwrap() {
            assert_eq!(start_row_id as usize, size * 8 + size / 2);
        }
    }
}
