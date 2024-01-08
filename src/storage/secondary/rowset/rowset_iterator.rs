// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::ops::Bound;
use std::sync::Arc;

use bitvec::prelude::BitVec;
use smallvec::smallvec;

use super::super::{ColumnIteratorImpl, ColumnSeekPosition, SecondaryIteratorImpl};
use super::DiskRowset;
use crate::array::ArrayImpl;
use crate::storage::secondary::DeleteVector;
use crate::storage::{KeyRange, PackedVec, StorageChunk, StorageColumnRef, StorageResult};

/// When `expected_size` is not specified, we should limit the maximum size of the chunk.
const ROWSET_MAX_OUTPUT: usize = 2048;

/// Iterates on a `RowSet`
pub struct RowSetIterator {
    column_refs: Arc<[StorageColumnRef]>,
    dvs: Vec<Arc<DeleteVector>>,
    column_iterators: Vec<ColumnIteratorImpl>,
    /// An optional filter for the first column.
    filter: Option<KeyRange>,
    /// Indicate whether the iterator has reached the end.
    end: bool,
}

impl RowSetIterator {
    pub async fn new(
        rowset: Arc<DiskRowset>,
        column_refs: Arc<[StorageColumnRef]>,
        dvs: Vec<Arc<DeleteVector>>,
        seek_pos: ColumnSeekPosition,
        filter: Option<KeyRange>,
    ) -> StorageResult<Self> {
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

        let mut column_iterators: Vec<ColumnIteratorImpl> = vec![];

        for column_ref in &*column_refs {
            // TODO: parallel seek
            match column_ref {
                StorageColumnRef::RowHandler => {
                    let column = rowset.column(0);
                    let row_count = column
                        .index()
                        .indexes()
                        .iter()
                        .fold(0, |acc, index| acc + index.row_count);
                    column_iterators.push(ColumnIteratorImpl::new_row_handler(
                        rowset.rowset_id(),
                        row_count,
                        start_row_id,
                    )?)
                }
                StorageColumnRef::Idx(idx) => column_iterators.push(
                    ColumnIteratorImpl::new(
                        rowset.column(*idx as usize),
                        rowset.column_info(*idx as usize),
                        start_row_id,
                    )
                    .await?,
                ),
            };
        }

        Ok(Self {
            column_refs,
            dvs,
            column_iterators,
            filter,
            end: false,
        })
    }

    /// Reads the next batch.
    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<StorageChunk>> {
        while !self.end {
            if let Some(batch) = self.next_batch_inner(expected_size).await? {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }

    /// Reads the next batch. This function may return `None` if the next batch is empty, but it
    /// doesn't mean that the iterator has reached the end.
    async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<StorageChunk>> {
        if self.end {
            return Ok(None);
        }
        // It's guaranteed that `expected_size` <= the number of items left
        // in the current block, if provided
        let mut fetch_size = {
            // We find the minimum fetch hints from the column iterators first
            let mut min: Option<usize> = None;
            let mut is_finished = true;
            for it in &self.column_iterators {
                let (hint, finished) = it.fetch_hint();
                if !finished {
                    is_finished = false;
                }
                if hint != 0 {
                    if let Some(v) = min {
                        min = Some(v.min(hint));
                    } else {
                        min = Some(hint);
                    }
                }
            }

            if let Some(min) = min {
                min.min(ROWSET_MAX_OUTPUT)
            } else {
                // Fast return: when all columns size is `0`, only has tow case:
                // 1. index of current block is no data can fetch (use `ROWSET_MAX_OUTPUT`).
                // 2. all columns is finished (return directly).
                if is_finished {
                    self.end = true;
                    return Ok(None);
                }
                ROWSET_MAX_OUTPUT
            }
        };
        if let Some(x) = expected_size {
            // Then, if `expected_size` is available, let `fetch_size`
            // be the min(fetch_size, expected_size)
            fetch_size = if x > fetch_size { fetch_size } else { x }
        }

        // TODO: parallel fetch
        // TODO: align unmatched rows

        // The visibility_map would probably be changed twice during scan process:
        //   1) if this rowset has delete vectors, that would be applied on it
        //   2) if filter needed, the filtered rows would be absent from the map
        // Otherwise, it retains `None` to indicate that at the current stage,
        // we would scan all rows and won't skip any block.
        let mut visibility_map = None;

        // Generate the initial visibility_map from delete vectors, so
        // that we can avoid unnecessary procedure if all rows have been
        // deleted in this batch
        if !self.dvs.is_empty() {
            // Get the start row id first
            let start_row_id = self.column_iterators[0].fetch_current_row_id();

            // Initialize visibility map and apply delete vector to it
            let mut visi = BitVec::new();
            visi.resize(fetch_size, true);
            for dv in &self.dvs {
                dv.apply_to(&mut visi, start_row_id);
            }

            // All rows in this batch have been deleted, call `skip`
            // on every columns
            if visi.not_any() {
                for (id, _) in self.column_refs.iter().enumerate() {
                    self.column_iterators[id].skip(visi.len());
                }
                return Ok(None);
            }

            // Switch visibility_map
            visibility_map = Some(visi);
        }

        let mut arrays: PackedVec<ArrayImpl> = smallvec![];
        // to make sure all columns have the same chunk range
        let mut common_chunk_range = None;

        // At this stage, we know that some rows survived from the filter scan if happend, so
        // just fetch the next batch for every other columns, and we have `visibility_map` to
        // indicate the visibility of its rows
        // TODO: Implement the skip interface for column_iterator and call it here.
        // For those already fetched columns, they also need to delete corrensponding blocks.
        for (id, _) in self.column_refs.iter().enumerate() {
            let Some((row_id, array)) = self.column_iterators[id]
                .next_batch(Some(fetch_size))
                .await?
            else {
                self.end = true;
                return Ok(None);
            };

            // check chunk range
            let current_range = row_id..row_id + array.len() as u32;
            if let Some(common_range) = &common_chunk_range {
                if common_range != &current_range {
                    panic!(
                        "unmatched row range from column iterator: {:?} of [{:?}], {:?} != {:?}",
                        self.column_refs[id], self.column_refs, common_range, current_range
                    );
                }
            } else {
                common_chunk_range = Some(current_range);
            }

            // For now, we only support range-filter scan by first column.
            if let Some(range) = &self.filter
                && id == 0
            {
                let len = array.len();
                let start_row_id = match &range.start {
                    Bound::Included(key) => (0..array.len()).position(|idx| &array.get(idx) >= key),
                    Bound::Excluded(key) => (0..array.len()).position(|idx| &array.get(idx) > key),
                    Bound::Unbounded => Some(0),
                }
                .unwrap_or(len);
                let end_row_id = match &range.end {
                    Bound::Included(key) => (0..array.len()).position(|idx| &array.get(idx) > key),
                    Bound::Excluded(key) => (0..array.len()).position(|idx| &array.get(idx) >= key),
                    Bound::Unbounded => None,
                }
                .unwrap_or(len);
                if (start_row_id..end_row_id) != (0..len) {
                    let bitmap = (0..len)
                        .map(|i| (start_row_id..end_row_id).contains(&i))
                        .collect();
                    if let Some(ref mut vis) = visibility_map {
                        *vis &= bitmap;
                    } else {
                        visibility_map = Some(bitmap);
                    }
                }
                if end_row_id == 0 {
                    self.end = true;
                }
            }

            arrays.push(array);
        }

        Ok(StorageChunk::construct(visibility_map, arrays))
    }
}

impl SecondaryIteratorImpl for RowSetIterator {}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use itertools::Itertools;

    use super::*;
    use crate::array::{Array, ArrayToVecExt};
    use crate::storage::secondary::rowset::tests::{
        helper_build_rowset, helper_build_rowset_with_first_key_recorded,
    };
    use crate::storage::secondary::SecondaryRowHandler;
    use crate::types::DataValue;

    #[tokio::test]
    async fn test_rowset_iterator() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = Arc::new(helper_build_rowset(&tempdir, false, 1000).await);
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
                None,
            )
            .await
            .unwrap();
        // 1 block contains 20 rows, so only 20 rows will be returned if `expected_size` > 20 here
        let chunk = it.next_batch(Some(1000)).await.unwrap().unwrap();
        if let ArrayImpl::Int32(array) = chunk.array_at(2) {
            let left = array.to_vec();
            let right = [1, 2, 3]
                .iter()
                .cycle()
                .cloned()
                .take(20)
                .map(Some)
                .collect_vec();
            assert_eq!(left, right);
        } else {
            unreachable!()
        }

        if let ArrayImpl::Int32(array) = chunk.array_at(1) {
            let left = array.to_vec();
            let right = [2, 3, 3, 3, 3, 3, 3]
                .iter()
                .cycle()
                .cloned()
                .take(20)
                .map(Some)
                .collect_vec();
            assert_eq!(left, right);
        } else {
            unreachable!()
        }

        if let ArrayImpl::Int64(array) = chunk.array_at(0) {
            assert_eq!(array.get(0), Some(&SecondaryRowHandler(0, 1000).as_i64()))
        } else {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_rowset_iterator_with_filter() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = Arc::new(helper_build_rowset(&tempdir, false, 1000).await);

        // v3 > 4: it.next_batch will return none, because StorageChunk::construct always return
        // none. v3 > 2: all blocks will be fetched.
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
                Some(KeyRange {
                    start: Bound::Excluded(DataValue::Int32(2)),
                    end: Bound::Unbounded,
                }),
            )
            .await
            .unwrap();
        let chunk = it.next_batch(Some(1000)).await.unwrap().unwrap();
        if let ArrayImpl::Int32(array) = chunk.array_at(2) {
            let left = array.to_vec();
            let right = [1, 2, 3]
                .iter()
                .cycle()
                .cloned()
                .take(20)
                .map(Some)
                .collect_vec();
            assert_eq!(left, right);
        } else {
            unreachable!()
        }

        if let ArrayImpl::Int32(array) = chunk.array_at(1) {
            let left = array.to_vec();
            let right = [2, 3, 3, 3, 3, 3, 3]
                .iter()
                .cycle()
                .cloned()
                .take(20)
                .map(Some)
                .collect_vec();
            assert_eq!(left, right);
        } else {
            unreachable!()
        }

        if let ArrayImpl::Int64(array) = chunk.array_at(0) {
            assert_eq!(array.get(0), Some(&SecondaryRowHandler(0, 1000).as_i64()))
        } else {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_rowset_iterator_with_range_filter() {
        {
            let tempdir = tempfile::tempdir().unwrap();
            let rowset = Arc::new(helper_build_rowset_with_first_key_recorded(&tempdir).await);
            let mut it = rowset
                .iter(
                    vec![
                        StorageColumnRef::Idx(0),
                        StorageColumnRef::Idx(1),
                        StorageColumnRef::Idx(2),
                    ]
                    .into(),
                    vec![],
                    ColumnSeekPosition::RowId(168),
                    Some(KeyRange {
                        start: Bound::Included(DataValue::Int32(180)),
                        end: Bound::Included(DataValue::Int32(195)),
                    }),
                )
                .await
                .unwrap();

            let mut column0_left = vec![];
            let mut column1_left = vec![];
            let mut column2_left = vec![];
            loop {
                let chunk = it.next_batch(None).await.unwrap();
                if chunk.is_none() {
                    break;
                }

                let storage_chunk = chunk.unwrap();
                data_from_chunk(&storage_chunk, &mut column0_left, 0).await;
                data_from_chunk(&storage_chunk, &mut column1_left, 1).await;
                data_from_chunk(&storage_chunk, &mut column2_left, 2).await;
            }
            let column0_right: Vec<i32> = (180..=195).collect();
            assert_eq!(column0_left, column0_right);

            let column1_right: Vec<i32> = (181..=196).collect();
            assert_eq!(column1_left, column1_right);

            let column2_right: Vec<i32> = (182..=197).collect();
            assert_eq!(column2_left, column2_right);
        }
        {
            // test without setting `start_keys` and `end_keys`,
            let tempdir = tempfile::tempdir().unwrap();
            let rowset = Arc::new(helper_build_rowset_with_first_key_recorded(&tempdir).await);
            let mut it = rowset
                .iter(
                    vec![
                        StorageColumnRef::Idx(0),
                        StorageColumnRef::Idx(1),
                        StorageColumnRef::Idx(2),
                    ]
                    .into(),
                    vec![],
                    ColumnSeekPosition::RowId(0),
                    None,
                )
                .await
                .unwrap();

            let mut column0_left = vec![];
            let mut column1_left = vec![];
            let mut column2_left = vec![];
            loop {
                let chunk = it.next_batch(Some(280)).await.unwrap();
                if chunk.is_none() {
                    break;
                }

                let storage_chunk = chunk.unwrap();
                data_from_chunk(&storage_chunk, &mut column0_left, 0).await;
                data_from_chunk(&storage_chunk, &mut column1_left, 1).await;
                data_from_chunk(&storage_chunk, &mut column2_left, 2).await;
            }
            let column0_right: Vec<i32> = (0..=279).collect();
            assert_eq!(column0_left, column0_right);

            let column1_right: Vec<i32> = (1..=280).collect();
            assert_eq!(column1_left, column1_right);

            let column2_right: Vec<i32> = (2..=281).collect();
            assert_eq!(column2_left, column2_right);
        }
        {
            // test only setting `start_keys`
            let tempdir = tempfile::tempdir().unwrap();
            let rowset = Arc::new(helper_build_rowset_with_first_key_recorded(&tempdir).await);
            let mut it = rowset
                .iter(
                    vec![
                        StorageColumnRef::Idx(0),
                        StorageColumnRef::Idx(1),
                        StorageColumnRef::Idx(2),
                    ]
                    .into(),
                    vec![],
                    ColumnSeekPosition::RowId(168),
                    Some(KeyRange {
                        start: Bound::Included(DataValue::Int32(180)),
                        end: Bound::Unbounded,
                    }),
                )
                .await
                .unwrap();

            let mut column0_left = vec![];
            let mut column1_left = vec![];
            let mut column2_left = vec![];
            loop {
                let chunk = it.next_batch(Some(280)).await.unwrap();
                if chunk.is_none() {
                    break;
                }

                let storage_chunk = chunk.unwrap();
                data_from_chunk(&storage_chunk, &mut column0_left, 0).await;
                data_from_chunk(&storage_chunk, &mut column1_left, 1).await;
                data_from_chunk(&storage_chunk, &mut column2_left, 2).await;
            }
            let column0_right: Vec<i32> = (180..=279).collect();
            assert_eq!(column0_left, column0_right);

            let column1_right: Vec<i32> = (181..=280).collect();
            assert_eq!(column1_left, column1_right);

            let column2_right: Vec<i32> = (182..=281).collect();
            assert_eq!(column2_left, column2_right);
        }
        {
            // test only set `start_keys` but no data satisfied.
            let tempdir = tempfile::tempdir().unwrap();
            let rowset = Arc::new(helper_build_rowset_with_first_key_recorded(&tempdir).await);
            let mut it = rowset
                .iter(
                    vec![
                        StorageColumnRef::Idx(0),
                        StorageColumnRef::Idx(1),
                        StorageColumnRef::Idx(2),
                    ]
                    .into(),
                    vec![],
                    ColumnSeekPosition::RowId(252),
                    Some(KeyRange {
                        start: Bound::Included(DataValue::Int32(1800)),
                        end: Bound::Unbounded,
                    }),
                )
                .await
                .unwrap();

            let mut column0_left = vec![];
            let mut column1_left = vec![];
            let mut column2_left = vec![];
            loop {
                let chunk = it.next_batch(Some(280)).await.unwrap();
                if chunk.is_none() {
                    break;
                }

                let storage_chunk = chunk.unwrap();
                data_from_chunk(&storage_chunk, &mut column0_left, 0).await;
                data_from_chunk(&storage_chunk, &mut column1_left, 1).await;
                data_from_chunk(&storage_chunk, &mut column2_left, 2).await;
            }
            let column0_right: Vec<i32> = vec![];
            assert_eq!(column0_left, column0_right);

            let column1_right: Vec<i32> = vec![];
            assert_eq!(column1_left, column1_right);

            let column2_right: Vec<i32> = vec![];
            assert_eq!(column2_left, column2_right);
        }
        {
            // test only set `end_keys
            let tempdir = tempfile::tempdir().unwrap();
            let rowset = Arc::new(helper_build_rowset_with_first_key_recorded(&tempdir).await);
            let mut it = rowset
                .iter(
                    vec![
                        StorageColumnRef::Idx(0),
                        StorageColumnRef::Idx(1),
                        StorageColumnRef::Idx(2),
                    ]
                    .into(),
                    vec![],
                    ColumnSeekPosition::RowId(0),
                    Some(KeyRange {
                        start: Bound::Unbounded,
                        end: Bound::Included(DataValue::Int32(195)),
                    }),
                )
                .await
                .unwrap();

            let mut column0_left = vec![];
            let mut column1_left = vec![];
            let mut column2_left = vec![];
            loop {
                let chunk = it.next_batch(Some(280)).await.unwrap();
                if chunk.is_none() {
                    break;
                }

                let storage_chunk = chunk.unwrap();
                data_from_chunk(&storage_chunk, &mut column0_left, 0).await;
                data_from_chunk(&storage_chunk, &mut column1_left, 1).await;
                data_from_chunk(&storage_chunk, &mut column2_left, 2).await;
            }
            let column0_right: Vec<i32> = (0..=195).collect();
            assert_eq!(column0_left, column0_right);

            let column1_right: Vec<i32> = (1..=196).collect();
            assert_eq!(column1_left, column1_right);

            let column2_right: Vec<i32> = (2..=197).collect();
            assert_eq!(column2_left, column2_right);
        }
        {
            // test only set `end_keys` but all data satisfied.
            let tempdir = tempfile::tempdir().unwrap();
            let rowset = Arc::new(helper_build_rowset_with_first_key_recorded(&tempdir).await);
            let mut it = rowset
                .iter(
                    vec![
                        StorageColumnRef::Idx(0),
                        StorageColumnRef::Idx(1),
                        StorageColumnRef::Idx(2),
                    ]
                    .into(),
                    vec![],
                    ColumnSeekPosition::RowId(0),
                    Some(KeyRange {
                        start: Bound::Unbounded,
                        end: Bound::Included(DataValue::Int32(19500)),
                    }),
                )
                .await
                .unwrap();

            let mut column0_left = vec![];
            let mut column1_left = vec![];
            let mut column2_left = vec![];
            loop {
                let chunk = it.next_batch(Some(280)).await.unwrap();
                if chunk.is_none() {
                    break;
                }

                let storage_chunk = chunk.unwrap();
                data_from_chunk(&storage_chunk, &mut column0_left, 0).await;
                data_from_chunk(&storage_chunk, &mut column1_left, 1).await;
                data_from_chunk(&storage_chunk, &mut column2_left, 2).await;
            }
            let column0_right: Vec<i32> = (0..=279).collect();
            assert_eq!(column0_left, column0_right);

            let column1_right: Vec<i32> = (1..=280).collect();
            assert_eq!(column1_left, column1_right);

            let column2_right: Vec<i32> = (2..=281).collect();
            assert_eq!(column2_left, column2_right);
        }
    }

    async fn data_from_chunk(chunk: &StorageChunk, column: &mut Vec<i32>, index: usize) {
        if let ArrayImpl::Int32(array) = chunk.array_at(index) {
            let bit_map = match chunk.visibility() {
                Some(bitvec) => bitvec.clone(),
                None => BitVec::new(),
            };
            for (idx, val) in array.iter().enumerate() {
                if !bit_map.is_empty() && !bit_map[idx] {
                    continue;
                }
                let val = val.unwrap();
                column.push(*val);
            }
        }
    }
}
