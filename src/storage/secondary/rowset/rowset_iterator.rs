use bitvec::prelude::BitVec;
use smallvec::smallvec;

use crate::array::ArrayImpl;
use crate::storage::secondary::DeleteVector;
use crate::storage::{PackedVec, StorageChunk, StorageColumnRef};
use std::sync::Arc;

use super::super::{
    ColumnIteratorImpl, ColumnSeekPosition, RowHandlerSequencer, SecondaryIteratorImpl,
};
use super::DiskRowset;

/// Iterates on a `RowSet`
pub struct RowSetIterator {
    rowset: Arc<DiskRowset>,
    column_refs: Arc<[StorageColumnRef]>,
    dvs: Vec<Arc<DeleteVector>>,
    column_iterators: Vec<Option<ColumnIteratorImpl>>,
}

impl RowSetIterator {
    pub async fn new(
        rowset: Arc<DiskRowset>,
        column_refs: Arc<[StorageColumnRef]>,
        dvs: Vec<Arc<DeleteVector>>,
        seek_pos: ColumnSeekPosition,
    ) -> Self {
        let start_row_id = match seek_pos {
            ColumnSeekPosition::RowId(row_id) => row_id,
            _ => todo!(),
        };

        if column_refs.len() == 0 {
            panic!("no column to iterate")
        }

        let row_handler_count = column_refs
            .iter()
            .filter(|x| matches!(x, StorageColumnRef::RowHandler))
            .count();

        if row_handler_count > 1 {
            panic!("more than 1 row handler column")
        }

        if row_handler_count == column_refs.len() {
            panic!("no user column")
        }

        let mut column_iterators: Vec<Option<ColumnIteratorImpl>> = vec![];

        for column_ref in &*column_refs {
            // TODO: parallel seek
            match column_ref {
                StorageColumnRef::RowHandler => column_iterators.push(None),
                StorageColumnRef::Idx(idx) => column_iterators.push(Some(
                    ColumnIteratorImpl::new(
                        rowset.column(*idx as usize),
                        rowset.column_info(*idx as usize),
                        start_row_id,
                    )
                    .await,
                )),
            };
        }

        Self {
            rowset,
            column_iterators,
            dvs,
            column_refs,
        }
    }

    /// Return (finished, data chunk of the current iteration)
    ///
    /// It is possible that after applying the deletion map, the current data chunk contains no
    /// element. In this case, the chunk will not be returned to the upper layer.
    ///
    /// TODO: check the deletion map before actually fetching data from column iterators.
    pub async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> (bool, Option<StorageChunk>) {
        let fetch_size = if let Some(x) = expected_size {
            x
        } else {
            // When `expected_size` is not available, we try to dispatch
            // as little I/O as possible. We find the minimum fetch hints
            // from the column itertaors.
            let mut min = None;
            for it in self.column_iterators.iter().flatten() {
                let hint = it.fetch_hint();
                if hint != 0 {
                    if min.is_none() {
                        min = Some(hint);
                    } else {
                        min = Some(min.unwrap().min(hint));
                    }
                }
            }
            min.unwrap_or(65536)
        };

        let mut arrays: PackedVec<Option<ArrayImpl>> = smallvec![];
        let mut common_chunk_range = None;

        // TODO: parallel fetch
        // TODO: align unmatched rows

        // Fill column data
        for (id, column_ref) in self.column_refs.iter().enumerate() {
            match column_ref {
                StorageColumnRef::RowHandler => arrays.push(None),
                StorageColumnRef::Idx(_) => {
                    if let Some((row_id, array)) = self.column_iterators[id]
                        .as_mut()
                        .unwrap()
                        .next_batch(Some(fetch_size))
                        .await
                    {
                        if let Some(x) = common_chunk_range {
                            if x != (row_id, array.len()) {
                                panic!("unmatched rowid from column iterator");
                            }
                        }
                        common_chunk_range = Some((row_id, array.len()));
                        arrays.push(Some(array));
                    } else {
                        arrays.push(None);
                    }
                }
            }
        }

        let common_chunk_range = if let Some(common_chunk_range) = common_chunk_range {
            common_chunk_range
        } else {
            return (true, None);
        };

        // Fill RowHandlers
        for (id, column_ref) in self.column_refs.iter().enumerate() {
            if matches!(column_ref, StorageColumnRef::RowHandler) {
                arrays[id] = Some(
                    RowHandlerSequencer::sequence(
                        self.rowset.rowset_id(),
                        common_chunk_range.0,
                        common_chunk_range.1 as u32,
                    )
                    .into(),
                );
            }
        }

        // Generate visibility bitmap
        let visibility = if self.dvs.is_empty() {
            None
        } else {
            let mut vis = BitVec::new();
            vis.resize(common_chunk_range.1, true);
            for dv in &self.dvs {
                dv.apply_to(&mut vis, common_chunk_range.0);
            }
            Some(vis)
        };

        (
            false,
            StorageChunk::construct(
                visibility,
                arrays
                    .into_iter()
                    .map(Option::unwrap)
                    .map(Arc::new)
                    .collect(),
            ),
        )
    }

    pub async fn next_batch(&mut self, expected_size: Option<usize>) -> Option<StorageChunk> {
        loop {
            let (finished, batch) = self.next_batch_inner(expected_size).await;
            if finished {
                return None;
            } else if let Some(batch) = batch {
                return Some(batch);
            }
        }
    }
}

impl SecondaryIteratorImpl for RowSetIterator {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, ArrayToVecExt};
    use crate::storage::secondary::rowset::tests::helper_build_rowset;
    use crate::storage::secondary::SecondaryRowHandler;
    use itertools::Itertools;

    #[tokio::test]
    async fn test_rowset_iterator() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = Arc::new(helper_build_rowset(&tempdir, false).await);
        let mut it = rowset
            .iter(
                vec![
                    StorageColumnRef::RowHandler,
                    StorageColumnRef::Idx(2),
                    StorageColumnRef::Idx(0),
                ]
                .into(),
                vec![],
                ColumnSeekPosition::RowId(1000),
            )
            .await;
        let chunk = it.next_batch(Some(1000)).await.unwrap();
        if let ArrayImpl::Int32(array) = chunk.array_at(2).as_ref() {
            let left = array.to_vec();
            let right = [1, 2, 3]
                .iter()
                .cycle()
                .cloned()
                .take(1000)
                .map(Some)
                .collect_vec();
            assert_eq!(left.len(), right.len());
            assert_eq!(left, right);
        } else {
            unreachable!()
        }

        if let ArrayImpl::Int32(array) = chunk.array_at(1).as_ref() {
            let left = array.to_vec();
            let right = [2, 3, 3, 3, 3, 3, 3]
                .iter()
                .cycle()
                .cloned()
                .take(1000)
                .map(Some)
                .collect_vec();
            assert_eq!(left.len(), right.len());
            assert_eq!(left, right);
        } else {
            unreachable!()
        }

        if let ArrayImpl::Int64(array) = chunk.array_at(0).as_ref() {
            assert_eq!(array.get(0), Some(&SecondaryRowHandler(0, 1000).as_i64()))
        } else {
            unreachable!()
        }
    }
}
