use super::*;
use crate::physical_plan::CreateTablePhysicalPlan;

pub struct CreateTableExecutor {
    pub plan: CreateTablePhysicalPlan,
    pub env: GlobalEnvRef,
}

impl CreateTableExecutor {
    pub async fn execute(self) -> Result<ExecutorResult, ExecutorError> {
        self.env.storage.create_table(
            self.plan.database_id,
            self.plan.schema_id,
            &self.plan.table_name,
            &self.plan.column_descs,
        )?;
        Ok(ExecutorResult::Empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use crate::storage::InMemoryStorage;
    use crate::types::DataTypeKind;
    use std::sync::Arc;

    #[test]
    fn test_create() {
        let storage = InMemoryStorage::new();
        let catalog = storage.catalog().clone();
        let env = Arc::new(GlobalEnv {
            storage: Arc::new(storage),
        });
        let plan = CreateTablePhysicalPlan {
            database_id: 0,
            schema_id: 0,
            table_name: "t".into(),
            column_descs: vec![
                ColumnCatalog::new(0, "v1".into(), DataTypeKind::Int32.not_null().to_column()),
                ColumnCatalog::new(1, "v2".into(), DataTypeKind::Int32.not_null().to_column()),
            ],
        };
        let executor = CreateTableExecutor { plan, env: env };
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
        let col0 = table_ref.get_column_by_id(0).unwrap();
        let col1 = table_ref.get_column_by_id(1).unwrap();
        assert_eq!(
            col0,
            ColumnCatalog::new(0, "v1".into(), DataTypeKind::Int32.not_null().to_column())
        );
        assert_eq!(
            col1,
            ColumnCatalog::new(1, "v2".into(), DataTypeKind::Int32.not_null().to_column())
        );
    }
}
