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
            .map(|x| StorageColumnRef::Idx(x.column_id)) // FIXME: use index, not id
            .collect_vec();

        // TODO: append row handler?

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

        while let Some(x) = it.next_batch(None).await? {
            yield x;
        }
    }
}
