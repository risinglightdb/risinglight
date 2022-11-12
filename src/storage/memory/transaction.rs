// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;

use super::table::InMemoryTableInnerRef;
use super::{InMemoryRowHandler, InMemoryTable, InMemoryTxnIterator};
use crate::array::{ArrayBuilderImpl, ArrayImplBuilderPickExt, ArrayImplSortExt, DataChunk};
use crate::catalog::{find_sort_key_id, ColumnCatalog};
use crate::storage::{StorageColumnRef, StorageResult, Transaction};
use crate::types::DataValue;
use crate::v1::binder::BoundExpr;

/// A transaction running on `InMemoryStorage`.
pub struct InMemoryTransaction {
    /// Indicates whether the transaction is committed or aborted. If
    /// the [`InMemoryTransaction`] object is dropped without finishing,
    /// the transaction will panic.
    finished: bool,

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

    /// All information about columns
    column_infos: Arc<[ColumnCatalog]>,
}

impl InMemoryTransaction {
    pub(super) fn start(table: &InMemoryTable) -> StorageResult<Self> {
        let inner = table.inner.read().unwrap();
        Ok(Self {
            finished: false,
            buffer: vec![],
            delete_buffer: vec![],
            table: table.inner.clone(),
            snapshot: Arc::new(inner.get_all_chunks()),
            deleted_rows: Arc::new(inner.get_all_deleted_rows()),
            column_infos: table.columns.clone(),
        })
    }
}

/// If primary key is found in [`ColumnCatalog`], sort all in-memory data using that key.
fn sort_datachunk_by_pk(
    chunks: &Arc<Vec<DataChunk>>,
    column_infos: &[ColumnCatalog],
) -> Arc<Vec<DataChunk>> {
    if let Some(sort_key_id) = find_sort_key_id(column_infos) {
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
        let sorted_index = arrays[sort_key_id].get_sorted_indices();

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

impl Transaction for InMemoryTransaction {
    type TxnIteratorType = InMemoryTxnIterator;

    type RowHandlerType = InMemoryRowHandler;

    // TODO: remove this unused variable
    async fn scan(
        &self,
        begin_sort_key: &[DataValue],
        end_sort_key: &[DataValue],
        col_idx: &[StorageColumnRef],
        is_sorted: bool,
        reversed: bool,
        expr: Option<BoundExpr>,
    ) -> StorageResult<InMemoryTxnIterator> {
        assert!(expr.is_none(), "MemTxn doesn't support filter scan");
        assert!(!reversed, "reverse iterator is not supported for now");

        assert!(
            begin_sort_key.is_empty(),
            "sort_key is not supported in InMemoryEngine for now"
        );
        assert!(
            end_sort_key.is_empty(),
            "sort_key is not supported in InMemoryEngine for now"
        );

        let snapshot = if is_sorted {
            sort_datachunk_by_pk(&self.snapshot, &self.column_infos)
        } else {
            self.snapshot.clone()
        };

        Ok(InMemoryTxnIterator::new(
            snapshot,
            self.deleted_rows.clone(),
            col_idx,
        ))
    }

    async fn append(&mut self, columns: DataChunk) -> StorageResult<()> {
        self.buffer.push(columns);
        Ok(())
    }

    async fn delete(&mut self, id: &Self::RowHandlerType) -> StorageResult<()> {
        self.delete_buffer.push(id.0 as usize);
        Ok(())
    }

    async fn commit(mut self) -> StorageResult<()> {
        let mut table = self.table.write().unwrap();
        for chunk in self.buffer.drain(..) {
            table.append(chunk)?;
        }
        for deletion in self.delete_buffer.drain(..) {
            table.delete(deletion)?;
        }

        self.finished = true;
        Ok(())
    }

    async fn abort(mut self) -> StorageResult<()> {
        self.finished = true;
        Ok(())
    }
}
