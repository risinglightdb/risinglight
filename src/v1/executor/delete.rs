// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::array::DataChunk;
use crate::catalog::TableRefId;
use crate::storage::{RowHandler, Storage, Table, Transaction};

/// The executor of `delete` statement.
pub struct DeleteExecutor<S: Storage> {
    pub table_ref_id: TableRefId,
    pub storage: Arc<S>,
    pub child: BoxedExecutor,
}

impl<S: Storage> DeleteExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let table = self.storage.get_table(self.table_ref_id)?;
        let mut txn = table.update().await?;
        let mut cnt = 0;
        #[for_await]
        for chunk in self.child {
            // TODO: we do not need a filter executor. We can simply get the boolean value from
            // the child.
            let chunk = chunk?;
            let row_handlers = chunk.array_at(chunk.column_count() - 1);
            for row_handler_idx in 0..row_handlers.len() {
                let row_handler = <S::Transaction as Transaction>::RowHandlerType::from_column(
                    row_handlers,
                    row_handler_idx,
                );
                txn.delete(&row_handler).await?;
                cnt += 1;
            }
        }
        txn.commit().await?;

        yield DataChunk::single(cnt);
    }
}
