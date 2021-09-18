use super::*;
use crate::physical_plan::PhysicalCreateTable;

pub struct CreateTableExecutor {
    pub plan: PhysicalCreateTable,
    pub env: GlobalEnvRef,
}

impl CreateTableExecutor {
    pub async fn execute(self) -> Result<ExecutorResult, ExecutorError> {
        self.env.storage.create_table(
            self.plan.database_id,
            self.plan.schema_id,
            &self.plan.table_name,
            &self.plan.columns,
        )?;
        Ok(ExecutorResult::Empty)
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
        let storage = InMemoryStorage::new();
        let catalog = storage.catalog().clone();
        let env = Arc::new(GlobalEnv {
            storage: Arc::new(storage),
        });
        let plan = PhysicalCreateTable {
            database_id: 0,
            schema_id: 0,
            table_name: "t".into(),
            columns: vec![
                ColumnCatalog::new(0, "v1".into(), DataTypeKind::Int.not_null().to_column()),
                ColumnCatalog::new(1, "v2".into(), DataTypeKind::Int.not_null().to_column()),
            ],
        };
        let executor = CreateTableExecutor { plan, env };
        futures::executor::block_on(executor.execute()).unwrap();

        let id = TableRefId {
            database_id: 0,
            schema_id: 0,
            table_id: 0,
        };
        assert_eq!(
            catalog.get_table_id(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t"),
            Some(id)
        );

        let table_ref = catalog.get_table(&id);
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
