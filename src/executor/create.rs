use super::*;
use crate::physical_plan::PhysicalCreateTable;

pub struct CreateTableExecutor {
    pub plan: PhysicalCreateTable,
    pub storage: StorageRef,
}

impl CreateTableExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            self.storage.create_table(
                self.plan.database_id,
                self.plan.schema_id,
                &self.plan.table_name,
                &self.plan.columns,
            )?;
            yield DataChunk::builder().cardinality(1).build();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use crate::storage::InMemoryStorage;
    use crate::types::{DataTypeExt, DataTypeKind};
    use std::sync::Arc;

    #[test]
    fn test_create() {
        let storage = Arc::new(InMemoryStorage::new());
        let catalog = storage.catalog().clone();
        let plan = PhysicalCreateTable {
            database_id: 0,
            schema_id: 0,
            table_name: "t".into(),
            columns: vec![
                ColumnCatalog::new(0, "v1".into(), DataTypeKind::Int.not_null().to_column()),
                ColumnCatalog::new(1, "v2".into(), DataTypeKind::Int.not_null().to_column()),
            ],
        };
        let mut executor = CreateTableExecutor { plan, storage }.execute().boxed();
        futures::executor::block_on(executor.next())
            .unwrap()
            .unwrap();

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
            ColumnCatalog::new(0, "v1".into(), DataTypeKind::Int.not_null().to_column())
        );
        assert_eq!(
            table_ref.get_column_by_id(1).unwrap(),
            ColumnCatalog::new(1, "v2".into(), DataTypeKind::Int.not_null().to_column())
        );
    }
}
