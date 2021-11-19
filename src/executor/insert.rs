use super::*;
use crate::array::DataChunk;
use crate::catalog::TableRefId;
use crate::storage::{Storage, Table, Transaction};
use crate::types::ColumnId;
use std::sync::Arc;

/// The executor of `insert` statement.
pub struct InsertExecutor<S: Storage> {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub storage: Arc<S>,
    pub child: BoxedExecutor,
}

impl<S: Storage> InsertExecutor<S> {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let table = self.storage.get_table(self.table_ref_id)?;
            let mut txn = table.write().await?;
            let mut cnt = 0;
            for await chunk in self.child {
                let chunk = chunk?;
                cnt += chunk.cardinality();
                txn.append(chunk).await?;
            }
            txn.commit().await?;

            yield DataChunk::single(cnt as i32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{ArrayImpl, DataChunk};
    use crate::catalog::{ColumnCatalog, TableRefId};
    use crate::executor::{CreateTableExecutor, GlobalEnv, GlobalEnvRef};
    use crate::physical_planner::PhysicalCreateTable;
    use crate::storage::InMemoryStorage;
    use crate::types::{DataTypeExt, DataTypeKind};
    use std::sync::Arc;

    #[tokio::test]
    async fn simple() {
        let env = create_table().await;
        let executor = InsertExecutor {
            table_ref_id: TableRefId::new(0, 0, 0),
            column_ids: vec![0, 1],
            storage: env.storage.as_in_memory_storage(),
            child: try_stream! {
                yield [
                    ArrayImpl::Int32((0..4).collect()),
                    ArrayImpl::Int32((100..104).collect()),
                ]
                .into_iter()
                .collect::<DataChunk>();
            }
            .boxed(),
        };
        executor.execute().boxed().next().await.unwrap().unwrap();
    }

    async fn create_table() -> GlobalEnvRef {
        let env = Arc::new(GlobalEnv {
            storage: StorageImpl::InMemoryStorage(Arc::new(InMemoryStorage::new())),
        });
        let plan = PhysicalCreateTable {
            database_id: 0,
            schema_id: 0,
            table_name: "t".into(),
            columns: vec![
                ColumnCatalog::new(
                    0,
                    "v1".into(),
                    DataTypeKind::Int(None).not_null().to_column(),
                ),
                ColumnCatalog::new(
                    1,
                    "v2".into(),
                    DataTypeKind::Int(None).not_null().to_column(),
                ),
            ],
        };
        let mut executor = CreateTableExecutor {
            plan,
            storage: env.storage.as_in_memory_storage(),
        }
        .execute()
        .boxed();
        executor.next().await.unwrap().unwrap();
        env
    }
}
