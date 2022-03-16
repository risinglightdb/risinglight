// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::array::DataChunk;
use crate::catalog::TableRefId;
use crate::storage::{RowHandler, Storage, Table, Transaction};

/// The executor of `delete` statement.
pub struct DeleteExecutor<S: Storage> {
    pub context: Arc<Context>,
    pub table_ref_id: TableRefId,
    pub storage: Arc<S>,
    pub child: BoxedExecutor,
}

impl<S: Storage> DeleteExecutor<S> {
    async fn execute_inner(self, token: CancellationToken) -> Result<i32, ExecutorError> {
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
                let row_handler = <S::TransactionType as Transaction>::RowHandlerType::from_column(
                    row_handlers,
                    row_handler_idx,
                );
                match unified_select_with_token(&token, txn.delete(&row_handler)).await {
                    Err(err) => {
                        txn.abort().await?;
                        return Err(err);
                    }
                    _ => {
                        cnt += 1;
                    }
                }
            }
        }
        txn.commit().await?;

        Ok(cnt as i32)
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let context = self.context.clone();
        match context.spawn(|token| async move { self.execute_inner(token).await }) {
            Some(handler) => {
                let cnt = handler.await.expect("failed to join delete thread")?;
                let chunk = DataChunk::single(cnt as i32);
                yield chunk;
            }
            None => return Err(ExecutorError::Abort),
        }
    }
}
