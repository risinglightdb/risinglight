// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::storage::Storage;
use crate::v1::optimizer::plan_nodes::PhysicalCreateTable;

/// The executor of `create table` statement.
pub struct CreateTableExecutor<S: Storage> {
    pub plan: PhysicalCreateTable,
    pub storage: Arc<S>,
}

impl<S: Storage> CreateTableExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        self.storage
            .create_table(
                self.plan.logical().database_id(),
                self.plan.logical().schema_id(),
                self.plan.logical().table_name(),
                self.plan.logical().columns(),
                self.plan.logical().ordered_pk_ids(),
            )
            .await?;

        let chunk = DataChunk::single(0);
        yield chunk
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::{ColumnCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use crate::storage::InMemoryStorage;
    use crate::types::DataTypeKind;
    use crate::v1::optimizer::plan_nodes::PhysicalCreateTable;

    #[tokio::test]
    async fn test_create() {
        {
            let storage = Arc::new(InMemoryStorage::new());
            let catalog = storage.catalog().clone();
            let plan = PhysicalCreateTable::new(LogicalCreateTable::new(
                0,
                0,
                "t".into(),
                vec![
                    ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("v1".into())),
                    ColumnCatalog::new(1, DataTypeKind::Int32.not_null().to_column("v2".into())),
                ],
                vec![],
            ));
            let mut executor = CreateTableExecutor { plan, storage }.execute().boxed();
            executor.next().await.unwrap().unwrap();

            let id = TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            };
            assert_eq!(
                catalog.get_table_id_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t"),
                Some(id)
            );

            let table_ref = catalog.get_table(&id).unwrap();
            assert_eq!(
                table_ref.get_column_by_id(0).unwrap(),
                ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("v1".into()))
            );
            assert_eq!(
                table_ref.get_column_by_id(1).unwrap(),
                ColumnCatalog::new(1, DataTypeKind::Int32.not_null().to_column("v2".into()))
            );
        }
        {
            let storage = Arc::new(InMemoryStorage::new());
            let catalog = storage.catalog().clone();
            let plan = PhysicalCreateTable::new(LogicalCreateTable::new(
                0,
                0,
                "t".into(),
                vec![
                    ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("v1".into())),
                    ColumnCatalog::new(1, DataTypeKind::Int32.not_null().to_column("v2".into())),
                    ColumnCatalog::new(2, DataTypeKind::Int32.nullable().to_column("v3".into())),
                ],
                vec![0, 1],
            ));
            let mut executor = CreateTableExecutor { plan, storage }.execute().boxed();
            executor.next().await.unwrap().unwrap();
            let table_id = TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            };
            let table_ref = catalog.get_table(&table_id).unwrap();
            assert_eq!(table_ref.primary_keys(), vec![0, 1]);
        }
        {
            let storage = Arc::new(InMemoryStorage::new());
            let catalog = storage.catalog().clone();
            let plan = PhysicalCreateTable::new(LogicalCreateTable::new(
                0,
                0,
                "t".into(),
                vec![
                    ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("v1".into())),
                    ColumnCatalog::new(1, DataTypeKind::Int32.not_null().to_column("v2".into())),
                    ColumnCatalog::new(2, DataTypeKind::Int32.nullable().to_column("v3".into())),
                ],
                vec![1, 0],
            ));
            let mut executor = CreateTableExecutor { plan, storage }.execute().boxed();
            executor.next().await.unwrap().unwrap();
            let table_id = TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            };
            let table_ref = catalog.get_table(&table_id).unwrap();
            assert_eq!(table_ref.primary_keys(), vec![1, 0]);
        }
    }
}
