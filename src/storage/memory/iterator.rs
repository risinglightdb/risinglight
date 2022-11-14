// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use bitvec::prelude::BitVec;

use crate::array::{ArrayImpl, DataChunk, I64Array};
use crate::storage::{StorageColumnRef, StorageResult, TxnIterator};

/// An iterator over all data in a transaction.
///
/// TODO: Lifetime of the iterator should be bound to the transaction.
/// When the transaction end, accessing items inside iterator is UB.
/// To achieve this, we must enable GAT.
pub struct InMemoryTxnIterator {
    chunks: Arc<Vec<DataChunk>>,
    deleted_rows: Arc<HashSet<usize>>,
    col_idx: Vec<StorageColumnRef>,
    cnt: usize,
    row_cnt: usize,
}

impl InMemoryTxnIterator {
    pub(super) fn new(
        chunks: Arc<Vec<DataChunk>>,
        deleted_rows: Arc<HashSet<usize>>,
        col_idx: &[StorageColumnRef],
    ) -> Self {
        Self {
            chunks,
            col_idx: col_idx.to_vec(),
            cnt: 0,
            row_cnt: 0,
            deleted_rows,
        }
    }

    async fn next_batch_inner(
        &mut self,
        _expected_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        if self.cnt >= self.chunks.len() {
            Ok(None)
        } else {
            let selected_chunk = &self.chunks[self.cnt];

            let batch_range = self.row_cnt..(selected_chunk.cardinality() + self.row_cnt);
            let visibility = batch_range
                .clone()
                .map(|x| !self.deleted_rows.contains(&x))
                .collect::<BitVec>();

            let chunk = if self.col_idx.is_empty() {
                DataChunk::no_column(visibility.count_ones())
            } else {
                self.col_idx
                    .iter()
                    .map(|idx| match idx {
                        StorageColumnRef::Idx(idx) => selected_chunk
                            .array_at(*idx as usize)
                            .filter(visibility.iter().map(|x| *x)),
                        StorageColumnRef::RowHandler => ArrayImpl::new_int64(I64Array::from_iter(
                            batch_range.clone().map(|x| x as i64),
                        ))
                        .filter(visibility.iter().map(|x| *x)),
                    })
                    .collect::<DataChunk>()
            };

            self.cnt += 1;
            self.row_cnt += selected_chunk.cardinality();

            Ok(Some(chunk))
        }
    }
}

impl TxnIterator for InMemoryTxnIterator {
    async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        self.next_batch_inner(expected_size).await
    }
}
