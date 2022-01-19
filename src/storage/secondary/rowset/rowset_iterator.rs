// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use bitvec::prelude::BitVec;
use smallvec::smallvec;

use super::super::{
    ColumnIteratorImpl, ColumnSeekPosition, RowHandlerSequencer, SecondaryIteratorImpl,
};
use super::DiskRowset;
use crate::array::{Array, ArrayImpl};
use crate::binder::BoundExpr;
use crate::storage::secondary::DeleteVector;
use crate::storage::{PackedVec, StorageChunk, StorageColumnRef, StorageResult};

/// When `expected_size` is not specified, we should limit the maximum size of the chunk.
const ROWSET_MAX_OUTPUT: usize = 65536;

/// Iterates on a `RowSet`
pub struct RowSetIterator {
    rowset: Arc<DiskRowset>,
    column_refs: Arc<[StorageColumnRef]>,
    dvs: Vec<Arc<DeleteVector>>,
    column_iterators: Vec<Option<ColumnIteratorImpl>>,
    filter_expr: Option<(BoundExpr, BitVec)>,
}

impl RowSetIterator {
    pub async fn new(
        rowset: Arc<DiskRowset>,
        column_refs: Arc<[StorageColumnRef]>,
        dvs: Vec<Arc<DeleteVector>>,
        seek_pos: ColumnSeekPosition,
        expr: Option<BoundExpr>,
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
                    .await?,
                )),
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
            rowset,
            column_refs,
            dvs,
            column_iterators,
            filter_expr,
        })
    }

    pub async fn next_batch_inner_with_filter(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<(bool, Option<StorageChunk>)> {
        let (expr, filter_column) = self.filter_expr.as_ref().unwrap();

        let fetch_size = if let Some(x) = expected_size {
            x
        } else {
            // When `expected_size` is not available, we try to dispatch
            // as little I/O as possible. We find the minimum fetch hints
            // from the column iterators.
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
            min.unwrap_or(ROWSET_MAX_OUTPUT)
        };

        let mut arrays: PackedVec<Option<ArrayImpl>> = smallvec![];
        let mut common_chunk_range = None;

        // TODO: parallel fetch
        // TODO: align unmatched rows

        for id in 0..filter_column.len() {
            if filter_column[id] {
                if let Some((row_id, array)) = self.column_iterators[id]
                    .as_mut()
                    .unwrap()
                    .next_batch(Some(fetch_size), None)
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
        for i in bool_array.iter() {
            if let Some(i) = i {
                filter_bitmap.push(*i);
            } else {
                filter_bitmap.push(false);
            }
        }

        // Apply dv to filter_bitmap
        if !self.dvs.is_empty() {
            for dv in &self.dvs {
                dv.apply_to(&mut filter_bitmap, common_chunk_range.0);
            }
        }

        if filter_bitmap.not_any() {
            for (id, column_ref) in self.column_refs.iter().enumerate() {
                match column_ref {
                    StorageColumnRef::RowHandler => continue,
                    StorageColumnRef::Idx(_) => {
                        if arrays[id].is_none() {
                            self.column_iterators[id]
                                .as_mut()
                                .unwrap()
                                .skip(filter_bitmap.len());
                        }
                    }
                }
            }
            return Ok((false, None));
        }

        // Use filter_bitmap to filter columns
        // TODO: Implement the skip interface for column_iterator and call it here.
        // For those already fetched columns, they also need to delete corrensponding blocks.
        for (id, column_ref) in self.column_refs.iter().enumerate() {
            match column_ref {
                StorageColumnRef::RowHandler => continue,
                StorageColumnRef::Idx(_) => {
                    if arrays[id].is_none() {
                        if let Some((row_id, array)) = self.column_iterators[id]
                            .as_mut()
                            .unwrap()
                            .next_batch(Some(fetch_size), Some(&filter_bitmap))
                            .await?
                        {
                            if common_chunk_range != (row_id, array.len()) {
                                panic!("unmatched rowid from column iterator");
                            }
                            arrays[id] = Some(array);
                        }
                    }
                }
            }
        }

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

        Ok((
            false,
            StorageChunk::construct(
                Some(filter_bitmap),
                arrays
                    .into_iter()
                    .map(Option::unwrap)
                    .map(Arc::new)
                    .collect(),
            ),
        ))
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
    ) -> StorageResult<(bool, Option<StorageChunk>)> {
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
            min.unwrap_or(ROWSET_MAX_OUTPUT)
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
                        .next_batch(Some(fetch_size), None)
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
                }
            }
        }

        let common_chunk_range = if let Some(common_chunk_range) = common_chunk_range {
            common_chunk_range
        } else {
            return Ok((true, None));
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

        Ok((
            false,
            StorageChunk::construct(
                visibility,
                arrays
                    .into_iter()
                    .map(Option::unwrap)
                    .map(Arc::new)
                    .collect(),
            ),
        ))
    }

    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<StorageChunk>> {
        loop {
            let (finished, batch) = if self.filter_expr.is_some() {
                self.next_batch_inner_with_filter(expected_size).await?
            } else {
                self.next_batch_inner(expected_size).await?
            };
            if finished {
                return Ok(None);
            } else if let Some(batch) = batch {
                return Ok(Some(batch));
            }
        }
    }
}

impl SecondaryIteratorImpl for RowSetIterator {}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use sqlparser::ast::BinaryOperator;
    pub use sqlparser::ast::DataType as DataTypeKind;

    use super::*;
    use crate::array::{Array, ArrayToVecExt};
    use crate::binder::{BoundBinaryOp, BoundInputRef};
    use crate::storage::secondary::rowset::tests::helper_build_rowset;
    use crate::storage::secondary::SecondaryRowHandler;
    use crate::types::{DataType, DataValue, PhysicalDataTypeKind};

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
        let chunk = it.next_batch(Some(1000)).await.unwrap().unwrap();
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

    #[tokio::test]
    async fn test_rowset_iterator_with_filter() {
        let tempdir = tempfile::tempdir().unwrap();
        let rowset = Arc::new(helper_build_rowset(&tempdir, false, 1000).await);

        // v3 > 4: it.next_batch will return none, because StorageChunk::construct always return
        // none. v3 > 2: all blocks will be fetched.
        let op = BinaryOperator::Gt;

        let left_expr = Box::new(BoundExpr::InputRef(BoundInputRef {
            index: 2,
            return_type: DataType {
                kind: DataTypeKind::Int(None),
                physical_kind: PhysicalDataTypeKind::Int32,
                nullable: true,
            },
        }));

        let right_expr = Box::new(BoundExpr::Constant(DataValue::Int32(2)));

        let return_type = Some(DataType {
            kind: DataTypeKind::Boolean,
            physical_kind: PhysicalDataTypeKind::Bool,
            nullable: true,
        });
        let expr = BoundExpr::BinaryOp(BoundBinaryOp {
            op,
            left_expr,
            right_expr,
            return_type,
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
            )
            .await
            .unwrap();
        let chunk = it.next_batch(Some(1000)).await.unwrap().unwrap();
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
