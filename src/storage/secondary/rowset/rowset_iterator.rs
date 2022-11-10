// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use bitvec::prelude::BitVec;
use smallvec::smallvec;

use super::super::{ColumnIteratorImpl, ColumnSeekPosition, SecondaryIteratorImpl};
use super::DiskRowset;
use crate::array::{Array, ArrayImpl};
use crate::storage::secondary::DeleteVector;
use crate::storage::{PackedVec, StorageChunk, StorageColumnRef, StorageResult};
use crate::types::DataValue;
use crate::v1::binder::BoundExpr;

/// When `expected_size` is not specified, we should limit the maximum size of the chunk.
const ROWSET_MAX_OUTPUT: usize = 2048;

/// Iterates on a `RowSet`
pub struct RowSetIterator {
    column_refs: Arc<[StorageColumnRef]>,
    dvs: Vec<Arc<DeleteVector>>,
    column_iterators: Vec<ColumnIteratorImpl>,
    filter_expr: Option<(BoundExpr, BitVec)>,
    start_keys: Vec<DataValue>,
    end_keys: Vec<DataValue>,
    meet_start_key_before: bool,
    meet_end_key_before: bool, // Indicate whether we have met `end_keys` in pre batch.
}
impl RowSetIterator {
    pub async fn new(
        rowset: Arc<DiskRowset>,
        column_refs: Arc<[StorageColumnRef]>,
        dvs: Vec<Arc<DeleteVector>>,
        seek_pos: ColumnSeekPosition,
        expr: Option<BoundExpr>,
        start_keys: &[DataValue],
        end_keys: &[DataValue],
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

        let filter_expr = if let Some(expr) = expr {
            let filter_column = expr.get_filter_column(column_refs.len());
            // assert filter column is not all false
            assert!(
                filter_column.any(),
                "There should be at least 1 filter column"
            );
            Some((expr, filter_column))
        } else {
            None
        };

        Ok(Self {
            column_refs,
            dvs,
            column_iterators,
            filter_expr,
            start_keys: start_keys.to_vec(),
            end_keys: end_keys.to_vec(),
            meet_end_key_before: false,
            meet_start_key_before: false,
        })
    }

    pub async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<(bool, Option<StorageChunk>)> {
        // We have met end key in pre `StorageChunk`
        // so we can finish cur scan.
        if self.meet_end_key_before {
            return Ok((true, None));
        }
        let filter_context = self.filter_expr.as_ref();
        // It's guaranteed that `expected_size` <= the number of items left
        // in the current block, if provided
        let mut fetch_size = {
            // We find the minimum fetch hints from the column iterators first
            let mut min = None;
            let mut is_finished = true;
            for it in &self.column_iterators {
                let (hint, finished) = it.fetch_hint();
                if !finished {
                    is_finished = false
                }
                if hint != 0 {
                    if min.is_none() {
                        min = Some(hint);
                    } else {
                        min = Some(min.unwrap().min(hint));
                    }
                }
            }

            if min.is_some() {
                min.unwrap().min(ROWSET_MAX_OUTPUT)
            } else {
                // Fast return: when all columns size is `0`, only has tow case:
                // 1. index of current block is no data can fetch (use `ROWSET_MAX_OUTPUT`).
                // 2. all columns is finished (return directly).
                if is_finished {
                    return Ok((true, None));
                }
                ROWSET_MAX_OUTPUT
            }
        };
        if let Some(x) = expected_size {
            // Then, if `expected_size` is available, let `fetch_size`
            // be the min(fetch_size, expected_size)
            fetch_size = if x > fetch_size { fetch_size } else { x }
        }

        let mut arrays: PackedVec<Option<ArrayImpl>> = smallvec![];
        let mut common_chunk_range = None;

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
                return Ok((false, None));
            }

            // Switch visibility_map
            visibility_map = Some(visi);
        }

        // Here, we scan the columns in filter condition if needed, if there are no
        // filter conditions, we don't do any modification to the `visibility_map`,
        // otherwise we apply the filtered result to it and get a new visibility map
        if let Some((expr, filter_columns)) = filter_context {
            for id in 0..filter_columns.len() {
                if filter_columns[id] {
                    if let Some((row_id, array)) = self.column_iterators[id]
                        .next_batch(Some(fetch_size))
                        .await?
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
                } else {
                    arrays.push(None);
                }
            }

            // This check is necessary
            let common_chunk_range = if let Some(common_chunk_range) = common_chunk_range {
                common_chunk_range
            } else {
                return Ok((true, None));
            };

            // Need to optimize
            let bool_array = match expr
                .eval_array_in_storage(&arrays, common_chunk_range.1)
                .unwrap()
            {
                ArrayImpl::Bool(a) => a,
                _ => panic!("filters can only accept bool array"),
            };

            let mut filter_bitmap = BitVec::with_capacity(bool_array.len());
            for (idx, e) in bool_array.iter().enumerate() {
                if let Some(visi) = visibility_map.as_ref() {
                    if !visi[idx] {
                        filter_bitmap.push(false);
                        continue;
                    }
                }
                if let Some(e) = e {
                    filter_bitmap.push(*e);
                } else {
                    filter_bitmap.push(false);
                }
            }

            // No rows left from the filter scan, skip columns which are not
            // in filter conditions
            if filter_bitmap.not_any() {
                for (id, _) in self.column_refs.iter().enumerate() {
                    if !filter_columns[id] {
                        self.column_iterators[id].skip(filter_bitmap.len());
                    }
                }
                return Ok((false, None));
            }
            visibility_map = Some(filter_bitmap);
        }
        // whether we have meet end key in cur scan.
        let mut meet_end_key = false;
        // At this stage, we know that some rows survived from the filter scan if happend, so
        // just fetch the next batch for every other columns, and we have `visibility_map` to
        // indicate the visibility of its rows
        // TODO: Implement the skip interface for column_iterator and call it here.
        // For those already fetched columns, they also need to delete corrensponding blocks.
        for (id, _) in self.column_refs.iter().enumerate() {
            if filter_context.is_none() {
                // If no filter, the `arrays` should be initialized here
                // manually by push a `None`
                arrays.push(None);
            }
            if arrays[id].is_none() {
                if let Some((row_id, array)) = self.column_iterators[id]
                    .next_batch(Some(fetch_size))
                    .await?
                {
                    if let Some(x) = common_chunk_range {
                        let current_data = (row_id, array.len());
                        if x != current_data {
                            panic!(
                                "unmatched rowid from column iterator: {:?} of [{:?}], {:?} != {:?}",
                                self.column_refs[id], self.column_refs, x, current_data
                            );
                        }
                    }
                    common_chunk_range = Some((row_id, array.len()));
                    arrays[id] = Some(array);
                }
            }
            // For now, we only support range-filter scan by first column.
            if id == 0 {
                if !self.start_keys.is_empty() && !self.meet_start_key_before {
                    // find the first row in range to begin with
                    self.meet_start_key_before = true;
                    let array = arrays[0].as_ref().unwrap();
                    let len = array.len();
                    let start_key = &self.start_keys[0];
                    let start_row_id =
                        (0..len).position(|idx| start_key - &array.get(idx) <= DataValue::Int32(0));
                    if start_row_id.is_none() {
                        // the `begin_key` is greater than all of the data, so on item survives in
                        // this scan
                        return Ok((true, None));
                    }
                    let start_row_id = start_row_id.unwrap();
                    let new_bitmap =
                        Self::mark_inaccessible(visibility_map.as_ref(), 0, start_row_id, len)
                            .await;
                    visibility_map = Some(new_bitmap);
                }

                if !self.end_keys.is_empty() && arrays[0].is_some() {
                    let array = arrays[0].as_ref().unwrap();
                    let len = array.len();
                    let end_key = &self.end_keys[0];
                    if end_key - &array.get(len - 1) < DataValue::Int32(0) {
                        // this block's last key is greater than the `end_key`,
                        // so we will finish scan after scan this block
                        meet_end_key = true;
                        let end_row_id = (0..len)
                            .position(|idx| end_key - &array.get(idx) < DataValue::Int32(0))
                            .unwrap();
                        let new_bitmap =
                            Self::mark_inaccessible(visibility_map.as_ref(), end_row_id, len, len)
                                .await;
                        visibility_map = Some(new_bitmap);
                    }
                }
            }
        }

        if common_chunk_range.is_none() {
            return Ok((true, None));
        };

        Ok((
            meet_end_key,
            StorageChunk::construct(
                visibility_map,
                arrays.into_iter().map(Option::unwrap).collect(),
            ),
        ))
    }

    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<StorageChunk>> {
        loop {
            let (finished, batch) = self.next_batch_inner(expected_size).await?;
            if finished {
                if batch.is_some() {
                    // we have met end key in cur batch, so we just return those data in the range.
                    self.meet_end_key_before = true;
                    return Ok(batch);
                }
                return Ok(None);
            } else if let Some(batch) = batch {
                return Ok(Some(batch));
            }
        }
    }

    /// mark all positions between `start_id`(include) and `end_id`(not include) false in a new
    /// `BitVec`, the len of this `BitVec` is `len`, and if a position is marked false in
    /// `bitmap`, we just keep in false in the new `Bitvec`
    pub async fn mark_inaccessible(
        bitmap: Option<&BitVec>,
        start_id: usize,
        end_id: usize,
        len: usize,
    ) -> BitVec {
        let mut filter_bitmap = BitVec::with_capacity(len);
        for idx in 0..len {
            if let Some(visi) = bitmap {
                if !visi[idx] {
                    // Cur row was previously marked inaccessible,
                    // so we'll just keep it.
                    filter_bitmap.push(false);
                    continue;
                }
            }
            if idx < end_id && idx >= start_id {
                filter_bitmap.push(false);
            } else {
                filter_bitmap.push(true);
            }
        }
        filter_bitmap
    }
}

impl SecondaryIteratorImpl for RowSetIterator {}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::array::{Array, ArrayToVecExt};
    use crate::storage::secondary::rowset::tests::{
        helper_build_rowset, helper_build_rowset_with_first_key_recorded,
    };
    use crate::storage::secondary::SecondaryRowHandler;
    use crate::types::{DataTypeKind, DataValue};
    use crate::v1::binder::{BoundBinaryOp, BoundInputRef};

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
                &[],
                &[],
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
            assert_eq!(left.len(), right.len());
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
            assert_eq!(left.len(), right.len());
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
        let op = BinaryOperator::Gt;

        let left_expr = Box::new(BoundExpr::InputRef(BoundInputRef {
            index: 2,
            return_type: DataTypeKind::Int32.nullable(),
        }));

        let right_expr = Box::new(BoundExpr::Constant(DataValue::Int32(2)));

        let expr = BoundExpr::BinaryOp(BoundBinaryOp {
            op,
            left_expr,
            right_expr,
            return_type: DataTypeKind::Bool.nullable(),
        });
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
                Some(expr),
                &[],
                &[],
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
            assert_eq!(left.len(), right.len());
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
            assert_eq!(left.len(), right.len());
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
            let start_keys = vec![DataValue::Int32(180)];
            let end_keys = vec![DataValue::Int32(195)];
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
                    None,
                    &start_keys,
                    &end_keys,
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
                    &[],
                    &[],
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
            let start_keys = vec![DataValue::Int32(180)];
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
                    None,
                    &start_keys,
                    &[],
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
            let start_keys = vec![DataValue::Int32(1800)];
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
                    None,
                    &start_keys,
                    &[],
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
            let end_keys = vec![DataValue::Int32(195)];
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
                    &[],
                    &end_keys,
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
            let end_keys = vec![DataValue::Int32(19500)];
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
                    &[],
                    &end_keys,
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
