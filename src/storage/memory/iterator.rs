// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use bitvec::prelude::BitVec;
use futures_async_stream::try_stream;

use crate::array::{ArrayImpl, DataChunk, I64Array};
use crate::storage::{StorageColumnRef, TracedStorageError};

/// An iterator over all data in a transaction.
///
/// TODO: Lifetime of the iterator should be bound to the transaction.
/// When the transaction end, accessing items inside iterator is UB.
/// To achieve this, we must enable GAT.
pub struct InMemoryTxnIterator {
    chunks: Arc<Vec<DataChunk>>,
    deleted_rows: Arc<HashSet<usize>>,
    col_idx: Vec<StorageColumnRef>,
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
            deleted_rows,
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = TracedStorageError)]
    pub async fn into_stream(self) {
        let mut row_cnt = 0;
        for selected_chunk in self.chunks.iter() {
            let batch_range = row_cnt..(selected_chunk.cardinality() + row_cnt);
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
                            .filter(&visibility.iter().map(|x| *x).collect::<Vec<bool>>()),
                        StorageColumnRef::RowHandler => ArrayImpl::new_int64(I64Array::from_iter(
                            batch_range.clone().map(|x| x as i64),
                        ))
                        .filter(&visibility.iter().map(|x| *x).collect::<Vec<bool>>()),
                    })
                    .collect::<DataChunk>()
            };

            row_cnt += selected_chunk.cardinality();

            yield chunk;
        }
    }
}
