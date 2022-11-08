// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::catalog::{ColumnCatalog, TableRefId};
use crate::storage::{Storage, Table, Transaction};
use crate::types::{ColumnId, ColumnIndex, DataType, DataValue};

/// The executor of `insert` statement.
pub struct InsertExecutor<S: Storage> {
    pub table_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub storage: Arc<S>,
}

impl<S: Storage> InsertExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        let table = self.storage.get_table(self.table_id)?;
        let columns = table.columns()?;
        let projs = columns
            .iter()
            .map(
                |col| match self.column_ids.iter().position(|&id| id == col.id()) {
                    Some(index) => Expr::ColumnIndex(ColumnIndex(index as _)),
                    None => Expr::Type(col.datatype().kind()),
                },
            )
            .collect_vec();

        let mut txn = table.write().await?;
        let mut cnt = 0;
        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            let chunk: DataChunk = projs
                .iter()
                .map(|col| match col {
                    Expr::ColumnIndex(idx) => chunk.array_at(idx.0 as _).clone(),
                    Expr::Type(ty) => {
                        let mut builder = ArrayBuilderImpl::with_capacity(
                            chunk.cardinality(),
                            &ty.clone().nullable(),
                        );
                        for _ in 0..chunk.cardinality() {
                            builder.push(&DataValue::Null);
                        }
                        builder.finish()
                    }
                    _ => unreachable!(),
                })
                .collect();
            cnt += chunk.cardinality();
            txn.append(chunk).await?;
        }
        txn.commit().await?;

        yield DataChunk::single(cnt as i32);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::ArrayImpl;
    use crate::catalog::{ColumnCatalog, TableRefId};
    use crate::storage::InMemoryStorage;
    use crate::types::DataTypeKind;

    #[tokio::test]
    async fn simple() {
        let storage = create_table().await;
        let executor = InsertExecutor {
            table_id: TableRefId::new(0, 0, 0),
            column_ids: vec![0, 1],
            storage: storage.as_in_memory_storage(),
        };
        let source = async_stream::try_stream! {
            yield [
                ArrayImpl::new_int32((0..4).collect()),
                ArrayImpl::new_int32((100..104).collect()),
            ]
            .into_iter()
            .collect();
        }
        .boxed();
        executor.execute(source).next().await.unwrap().unwrap();
    }

    async fn create_table() -> StorageImpl {
        let storage = StorageImpl::InMemoryStorage(Arc::new(InMemoryStorage::new()));
        storage
            .as_in_memory_storage()
            .create_table(
                0,
                0,
                "t",
                &[
                    ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("v1".into())),
                    ColumnCatalog::new(1, DataTypeKind::Int32.not_null().to_column("v2".into())),
                ],
                &[],
            )
            .await;
        storage
    }
}
