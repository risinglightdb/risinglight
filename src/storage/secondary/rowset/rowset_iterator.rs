// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use bitvec::prelude::BitVec;
use smallvec::smallvec;

use super::super::{ColumnIteratorImpl, ColumnSeekPosition, SecondaryIteratorImpl};
use super::DiskRowset;
use crate::array::{Array, ArrayImpl};
use crate::binder::BoundExpr;
use crate::storage::secondary::DeleteVector;
use crate::storage::{PackedVec, StorageChunk, StorageColumnRef, StorageResult};

/// When `expected_size` is not specified, we should limit the maximum size of the chunk.
const ROWSET_MAX_OUTPUT: usize = 2048;

/// Iterates on a `RowSet`
pub struct RowSetIterator {
    column_refs: Arc<[StorageColumnRef]>,
    dvs: Vec<Arc<DeleteVector>>,
    column_iterators: Vec<ColumnIteratorImpl>,
    filter_expr: Option<(BoundExpr, BitVec)>,
    end_sort_key: Option<Vec<u8>>,
}

impl RowSetIterator {
    pub async fn new(
        rowset: Arc<DiskRowset>,
        column_refs: Arc<[StorageColumnRef]>,
        dvs: Vec<Arc<DeleteVector>>,
        seek_pos: ColumnSeekPosition,
        expr: Option<BoundExpr>,
        end_sort_key: Option<&[u8]>,
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
        let end_sort_key = if end_sort_key.is_none() {
            None
        } else {
            Some(end_sort_key.unwrap().to_vec())
        };
        Ok(Self {
            column_refs,
            dvs,
            column_iterators,
            filter_expr,
            end_sort_key,
        })
    }

    pub async fn next_batch_inner(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<(bool, Option<StorageChunk>)> {
        let filter_context = self.filter_expr.as_ref();
        // It's guaranteed that `expected_size` <= the number of items left
        // in the current block, if provided
        let mut fetch_size = {
            // We find the minimum fetch hints from the column iterators first
            let mut min = None;
            for it in &self.column_iterators {
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
            let mut is_meet_end_key = false;
            for id in 0..filter_columns.len() {
                if filter_columns[id] {
                    if let Some((row_id, mut array)) = self.column_iterators[id]
                        .next_batch(Some(fetch_size))
                        .await?
                    {
                        if self.end_sort_key.is_some() && id == 0 {
                            let end_sort_key = self.end_sort_key.to_owned().unwrap();
                            if end_sort_key < array.get_to_string(array.len() - 1).into_bytes() {
                                is_meet_end_key = true;
                                for i in 0..array.len() {
                                    if end_sort_key < array.get_to_string(i).into_bytes() {
                                        array = array.slice(0..i);
                                        fetch_size = i;
                                    }
                                }
                            }
                        }

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
            if is_meet_end_key {
                return Ok((
                    true,
                    StorageChunk::construct(
                        visibility_map,
                        arrays.into_iter().map(Option::unwrap).collect(),
                    ),
                ));
            }
        }

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
        }

        if common_chunk_range.is_none() {
            return Ok((true, None));
        };

        Ok((
            false,
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
                None,
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
}
