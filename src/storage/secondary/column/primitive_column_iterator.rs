use std::marker::PhantomData;

use async_trait::async_trait;
use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;
use rust_decimal::Decimal;

use super::super::{
    Block, BlockIterator, PlainPrimitiveBlockIterator, PlainPrimitiveNullableBlockIterator,
    PrimitiveFixedWidthEncode,
};
use super::{Column, ColumnIterator, ColumnSeekPosition};
use crate::array::{Array, ArrayBuilder};

/// All supported block iterators for primitive types.
pub(super) enum BlockIteratorImpl<T: PrimitiveFixedWidthEncode> {
    Plain(PlainPrimitiveBlockIterator<T>),
    PlainNullable(PlainPrimitiveNullableBlockIterator<T>),
}

pub struct PrimitiveColumnIterator<T: PrimitiveFixedWidthEncode> {
    column: Column,
    current_block_id: u32,
    block_iterator: BlockIteratorImpl<T>,
    current_row_id: u32,
    finished: bool,
    _phantom: PhantomData<T>,
}

pub type I32ColumnIterator = PrimitiveColumnIterator<i32>;
pub type F64ColumnIterator = PrimitiveColumnIterator<f64>;
pub type BoolColumnIterator = PrimitiveColumnIterator<bool>;
pub type DecimalColumnIterator = PrimitiveColumnIterator<Decimal>;

impl<T: PrimitiveFixedWidthEncode> PrimitiveColumnIterator<T> {
    fn get_iterator_for(
        block_type: BlockType,
        block: Block,
        index: &BlockIndex,
        start_pos: usize,
    ) -> BlockIteratorImpl<T> {
        match block_type {
            BlockType::Plain => {
                let mut it = PlainPrimitiveBlockIterator::new(block, index.row_count as usize);
                it.skip(start_pos - index.first_rowid as usize);
                BlockIteratorImpl::Plain(it)
            }
            BlockType::PlainNullable => {
                let mut it =
                    PlainPrimitiveNullableBlockIterator::new(block, index.row_count as usize);
                it.skip(start_pos - index.first_rowid as usize);
                BlockIteratorImpl::PlainNullable(it)
            }
            _ => todo!(),
        }
    }

    pub async fn new(column: Column, start_pos: u32) -> Self {
        let current_block_id = column
            .index()
            .block_of_seek_position(ColumnSeekPosition::RowId(start_pos));
        let (header, block) = column.get_block(current_block_id).await;

        Self {
            block_iterator: Self::get_iterator_for(
                header.block_type,
                block,
                column.index().index(current_block_id),
                start_pos as usize,
            ),
            column,
            current_block_id,
            current_row_id: start_pos,
            finished: false,
            _phantom: PhantomData,
        }
    }

    pub async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> Option<(u32, T::ArrayType)> {
        if self.finished {
            return None;
        }

        let capacity = if let Some(expected_size) = expected_size {
            expected_size
        } else {
            match &mut self.block_iterator {
                BlockIteratorImpl::Plain(bi) => bi.remaining_items(),
                BlockIteratorImpl::PlainNullable(bi) => bi.remaining_items(),
            }
        };

        let mut builder = <T::ArrayType as Array>::Builder::with_capacity(capacity);
        let mut total_cnt = 0;
        let first_row_id = self.current_row_id;

        loop {
            let cnt = match &mut self.block_iterator {
                BlockIteratorImpl::Plain(bi) => {
                    bi.next_batch(expected_size.map(|x| x - total_cnt), &mut builder)
                }
                BlockIteratorImpl::PlainNullable(bi) => {
                    bi.next_batch(expected_size.map(|x| x - total_cnt), &mut builder)
                }
            };

            total_cnt += cnt;
            self.current_row_id += cnt as u32;

            if let Some(expected_size) = expected_size {
                if total_cnt >= expected_size {
                    break;
                }
            } else if total_cnt != 0 {
                break;
            }

            self.current_block_id += 1;

            if self.current_block_id >= self.column.index().len() as u32 {
                self.finished = true;
                break;
            }

            let (header, block) = self.column.get_block(self.current_block_id).await;
            self.block_iterator = Self::get_iterator_for(
                header.block_type,
                block,
                self.column.index().index(self.current_block_id),
                self.current_row_id as usize,
            );
        }

        if total_cnt == 0 {
            None
        } else {
            Some((first_row_id, builder.finish()))
        }
    }

    fn fetch_hint_inner(&self) -> usize {
        if self.finished {
            return 0;
        }
        let index = self.column.index().index(self.current_block_id);
        (index.row_count - (self.current_row_id - index.first_rowid)) as usize
    }
}

#[async_trait]
impl<T: PrimitiveFixedWidthEncode> ColumnIterator<T::ArrayType> for PrimitiveColumnIterator<T> {
    async fn next_batch(&mut self, expected_size: Option<usize>) -> Option<(u32, T::ArrayType)> {
        self.next_batch_inner(expected_size).await
    }

    fn fetch_hint(&self) -> usize {
        self.fetch_hint_inner()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::array::ArrayToVecExt;
    use crate::storage::secondary::rowset::tests::helper_build_rowset;
    use crate::storage::secondary::PrimitiveColumnIterator;

    #[tokio::test]
    async fn test_scan_i32() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir, false).await;
        let column = rowset.column(0);
        let mut scanner = PrimitiveColumnIterator::<i32>::new(column.clone(), 0).await;
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

        let mut scanner = PrimitiveColumnIterator::<i32>::new(column, 10000).await;
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
