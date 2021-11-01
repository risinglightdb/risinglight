use std::marker::PhantomData;

use super::column::Column;
use super::{
    Block, BlockIterator, ColumnIterator, ColumnSeekPosition, PlainPrimitiveBlockIterator,
    PrimitiveFixedWidthEncode,
};

use async_trait::async_trait;
use risinglight_proto::rowset::block_index::BlockType;

/// All supported block iterators for primitive types.
pub(super) enum BlockIteratorImpl<T: PrimitiveFixedWidthEncode> {
    Plain(PlainPrimitiveBlockIterator<T>),
}

pub struct PrimitiveColumnIterator<T: PrimitiveFixedWidthEncode> {
    column: Column,
    current_block_id: u32,
    block_iterator: BlockIteratorImpl<T>,
    finished: bool,
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PrimitiveColumnIterator<T> {
    fn get_iterator_for(
        block_type: BlockType,
        block: Block,
        length: usize,
    ) -> BlockIteratorImpl<T> {
        match block_type {
            BlockType::Plain => {
                BlockIteratorImpl::Plain(PlainPrimitiveBlockIterator::new(block, length))
            }
            _ => todo!(),
        }
    }

    pub async fn new(column: Column, seek_pos: ColumnSeekPosition) -> Self {
        let current_block_id = column.index().block_of_seek_position(seek_pos);
        let (header, block) = column.get_block(current_block_id).await;
        Self {
            block_iterator: Self::get_iterator_for(
                header.block_type,
                block,
                column.index().index(current_block_id).row_count as usize,
            ),
            column,
            current_block_id,
            finished: false,
            _phantom: PhantomData,
        }
    }

    pub async fn next_batch_inner(&mut self) -> Option<(u32, T::ArrayType)> {
        if self.finished {
            return None;
        }

        loop {
            let data = match &mut self.block_iterator {
                BlockIteratorImpl::Plain(bi) => bi.next_batch(),
            };

            if let Some(array) = data {
                return Some((
                    self.column.index().index(self.current_block_id).first_rowid,
                    array,
                ));
            }

            self.current_block_id += 1;

            if self.current_block_id >= self.column.index().len() as u32 {
                self.finished = true;
                return None;
            }

            let (header, block) = self.column.get_block(self.current_block_id).await;
            self.block_iterator = Self::get_iterator_for(
                header.block_type,
                block,
                self.column.index().index(self.current_block_id).row_count as usize,
            );
        }
    }
}

#[async_trait]
impl<T: PrimitiveFixedWidthEncode> ColumnIterator<T::ArrayType> for PrimitiveColumnIterator<T> {
    async fn next_batch(&mut self) -> Option<(u32, T::ArrayType)> {
        self.next_batch_inner().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::ArrayToVecExt;
    use crate::storage::secondary::rowset::primitive_column_iterator::PrimitiveColumnIterator;
    use crate::storage::secondary::tests::helper_build_rowset;
    use crate::storage::secondary::ColumnSeekPosition;
    use itertools::Itertools;

    #[tokio::test]
    async fn test_scan_i32() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = helper_build_rowset(&tempdir, false).await;
        let column = rowset.column(0);
        let mut scanner =
            PrimitiveColumnIterator::<i32>::new(column.clone(), ColumnSeekPosition::Start).await;
        let mut recv_data = vec![];
        while let Some((_, data)) = scanner.next_batch().await {
            recv_data.extend(data.to_vec());
        }
        assert_eq!(
            recv_data[..1000],
            [1, 2, 3]
                .iter()
                .cycle()
                .cloned()
                .take(1000)
                .map(Some)
                .collect_vec()
        );

        let mut scanner =
            PrimitiveColumnIterator::<i32>::new(column, ColumnSeekPosition::RowId(100000)).await;
        let (id, _) = scanner.next_batch().await.unwrap();
        // should not start from the first block, and should contain RowId 100000 in consequent batches.
        assert!(id <= 100000 && id != 0);
    }
}
