// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;

use super::table::InMemoryTableInnerRef;
use super::{InMemoryTable, InMemoryTxnIterator};
use crate::array::{ArrayBuilderImpl, ArrayImplBuilderPickExt, DataChunk};
use crate::storage::{
    BoxChunkStream, RowHandler, ScanOptions, StorageColumnRef, StorageResult, Table, Transaction,
};

/// A transaction running on `InMemoryStorage`.
pub struct InMemoryTransaction {
    /// Includes all to-be-committed data.
    buffer: Vec<DataChunk>,

    /// All rows to be deleted
    delete_buffer: Vec<usize>,

    /// When transaction is started, reference to all data chunks will
    /// be cached in `snapshot` to provide snapshot isolation.
    snapshot: Arc<Vec<DataChunk>>,

    /// Reference to inner table.
    table: InMemoryTableInnerRef,

    /// Snapshot of all deleted rows
    deleted_rows: Arc<HashSet<usize>>,

    /// Ordered primary key indexes in `column_infos`
    ordered_pk_idx: Vec<usize>,
}

impl InMemoryTransaction {
    pub(super) fn start(table: &InMemoryTable) -> StorageResult<Self> {
        let inner = table.inner.read().unwrap();
        let ordered_pk_idx = table
            .primary_key()
            .iter()
            .map(|id| {
                table
                    .columns
                    .iter()
                    .position(|c| c.id() == *id)
                    .expect("Malformed table object")
            })
            .collect_vec();
        Ok(Self {
            buffer: vec![],
            delete_buffer: vec![],
            table: table.inner.clone(),
            snapshot: Arc::new(inner.get_all_chunks()),
            deleted_rows: Arc::new(inner.get_all_deleted_rows()),
            ordered_pk_idx,
        })
    }
}

/// If primary key is found in [`ColumnCatalog`], sort all in-memory data using that key.
fn sort_datachunk_by_pk(
    chunks: &Arc<Vec<DataChunk>>,
    ordered_pk_idx: &[usize],
) -> Arc<Vec<DataChunk>> {
    if !ordered_pk_idx.is_empty() {
        if chunks.is_empty() {
            return chunks.clone();
        }
        let mut builders = chunks[0]
            .arrays()
            .iter()
            .map(ArrayBuilderImpl::from_type_of_array)
            .collect_vec();

        for chunk in &**chunks {
            for (array, builder) in chunk.arrays().iter().zip(builders.iter_mut()) {
                builder.append(array);
            }
        }

        let arrays = builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect_vec();

        let pk_arrays = Vec::from(ordered_pk_idx)
            .iter()
            .map(|idx| &arrays[*idx])
            .collect_vec();
        let pk_array = itertools::izip!(pk_arrays).collect_vec();
        let sorted_index = (0..pk_array.len())
            .sorted_by_key(|idx| pk_array[*idx])
            .collect_vec();

        let chunk = arrays
            .into_iter()
            .map(|array| {
                let mut builder = ArrayBuilderImpl::from_type_of_array(&array);
                builder.pick_from(&array, &sorted_index);
                builder.finish()
            })
            .collect::<DataChunk>();
        Arc::new(vec![chunk])
    } else {
        chunks.clone()
    }
}

#[async_trait::async_trait]
impl Transaction for InMemoryTransaction {
    // TODO: remove this unused variable
    async fn scan(
        &self,
        col_idx: &[StorageColumnRef],
        opts: ScanOptions,
    ) -> StorageResult<BoxChunkStream> {
        assert!(opts.filter.is_none(), "MemTxn doesn't support filter scan");
        assert!(!opts.reversed, "reverse iterator is not supported for now");

        let snapshot = if opts.is_sorted {
            sort_datachunk_by_pk(&self.snapshot, &self.ordered_pk_idx)
        } else {
            self.snapshot.clone()
        };

        Ok(InMemoryTxnIterator::new(snapshot, self.deleted_rows.clone(), col_idx).into_stream())
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.buffer.push(columns);
        Ok(())
    }

    async fn delete(&mut self, ids: &[RowHandler]) -> StorageResult<()> {
        for id in ids {
            self.delete_buffer.push(*id as usize);
        }
        Ok(())
    }

    async fn commit(&mut self) -> StorageResult<()> {
        let mut table = self.table.write().unwrap();
        for chunk in self.buffer.drain(..) {
            table.append(chunk)?;
        }
        for deletion in self.delete_buffer.drain(..) {
            table.delete(deletion)?;
        }
        Ok(())
    }
}
