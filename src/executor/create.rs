// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::optimizer::plan_nodes::PhysicalCreateTable;
use crate::storage::Storage;

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
            )
            .await?;

        let mut chunk = DataChunk::single(0);
        chunk.set_header(vec!["$create".to_string()]);
        yield chunk
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::{ColumnCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use crate::optimizer::plan_nodes::PhysicalCreateTable;
    use crate::storage::InMemoryStorage;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[tokio::test]
    async fn test_create() {
        let storage = Arc::new(InMemoryStorage::new());
        let catalog = storage.catalog().clone();
        let plan = PhysicalCreateTable::new(LogicalCreateTable::new(
            0,
            0,
            "t".into(),
            vec![
                ColumnCatalog::new(0, DataTypeKind::Int(None).not_null().to_column("v1".into())),
                ColumnCatalog::new(1, DataTypeKind::Int(None).not_null().to_column("v2".into())),
            ],
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
            ColumnCatalog::new(0, DataTypeKind::Int(None).not_null().to_column("v1".into()))
        );
        assert_eq!(
            table_ref.get_column_by_id(1).unwrap(),
            ColumnCatalog::new(1, DataTypeKind::Int(None).not_null().to_column("v2".into()))
        );
    }
}
