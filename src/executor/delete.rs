// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{ArrayImpl, DataChunk};
use crate::catalog::TableRefId;
use crate::storage::StorageRef;

/// The executor of `delete` statement.
///
/// The last column of the input data chunk should be `_row_id_`.
pub struct DeleteExecutor {
    pub table_id: TableRefId,
    pub storage: StorageRef,
}

impl DeleteExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        let table = self.storage.get_table(self.table_id).await?;
        let mut txn = table.update().await?;
        let mut cnt = 0;
        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            let ArrayImpl::Int64(rowids) = chunk.array_at(chunk.column_count() - 1) else {
                panic!("column _row_id_ should be i64 type");
            };
            txn.delete(rowids.values()).await?;
            cnt += chunk.cardinality();
        }
        txn.commit().await?;

        yield DataChunk::single(cnt as i32);
    }
}
