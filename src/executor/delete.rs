use super::*;
use crate::{
    array::DataChunk,
    catalog::TableRefId,
    storage::{RowHandler, Storage, Table, Transaction},
};
use std::sync::Arc;

/// The executor of `delete` statement.
pub struct DeleteExecutor<S: Storage> {
    pub table_ref_id: TableRefId,
    pub storage: Arc<S>,
    pub child: BoxedExecutor,
}

impl<S: Storage> DeleteExecutor<S> {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let table = self.storage.get_table(self.table_ref_id)?;
            let mut txn = table.update().await?;
            let mut cnt = 0;
            for await chunk in self.child {
                // TODO: we do not need a filter executor. We can simply get the boolean value from
                // the child.
                let chunk = chunk?;
                let row_handlers = chunk.array_at(chunk.column_count() - 1);
                for row_handler_idx in 0..row_handlers.len() {
                    let row_handler = <S::TransactionType as Transaction>::RowHandlerType::from_column(row_handlers, row_handler_idx);
                    txn.delete(&row_handler).await?;
                    cnt += 1;
                }
            }
            txn.commit().await?;

            yield DataChunk::single(cnt);
        }
    }
}
