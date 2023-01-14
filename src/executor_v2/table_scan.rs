// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::array::DataChunk;
use crate::catalog::{ColumnRefId, TableRefId};
use crate::storage::{Storage, StorageColumnRef, Table, Transaction, TxnIterator};

/// The executor of table scan operation.
pub struct TableScanExecutor<S: Storage> {
    pub table_id: TableRefId,
    pub columns: Vec<ColumnRefId>,
    pub storage: Arc<S>,
}

impl<S: Storage> TableScanExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let table = self.storage.get_table(self.table_id)?;

        let mut col_idx = self
            .columns
            .iter()
            .map(|x| match x.column_id {
                u32::MAX => StorageColumnRef::RowHandler,
                id => StorageColumnRef::Idx(id), // convert column id -> storage column idx
            })
            .collect_vec();

        // TODO: append row handler?
        if self.columns.is_empty() {
            col_idx.push(StorageColumnRef::RowHandler);
        }

        let txn = table.read().await?;

        let mut it = txn
            .scan(
                &[],
                &[],
                &col_idx,
                false, // TODO: is_sorted
                false,
                None, // TODO: support filter scan
            )
            .await?;

        while let Some(mut x) = it.next_batch(None).await? {
            if self.columns.is_empty() {
                x = DataChunk::no_column(x.cardinality());
            }
            yield x;
        }
    }
}
