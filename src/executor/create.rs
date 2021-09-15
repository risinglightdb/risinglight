use super::*;
use crate::physical_plan::CreateTablePhysicalPlan;
use crate::server::GlobalEnvRef;

pub struct CreateTableExecutor {
    pub plan: CreateTablePhysicalPlan,
    pub env: GlobalEnvRef,
}

impl CreateTableExecutor {
    pub async fn execute(self) -> Result<(), ExecutorError> {
        self.env
            .storage
            .create_table(
                self.plan.database_id,
                self.plan.schema_id,
                &self.plan.table_name,
                &self.plan.column_descs,
            )
            .map_err(|_| ExecutorError::CreateTableError)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::{Bind, Binder};
    use crate::catalog::{ColumnCatalog, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use crate::logical_plan::LogicalPlanGenerator;
    use crate::parser::SQLStatement;
    use crate::physical_plan::PhysicalPlanGenerator;
    use crate::server::GlobalEnv;
    use crate::storage::InMemoryStorage;
    use crate::types::DataTypeKind;
    use std::sync::Arc;

    #[test]
    fn test_create() {
        let storage = InMemoryStorage::new();
        let catalog = storage.catalog().clone();
        let mut binder = Binder::new(catalog.clone());
        let global_env = Arc::new(GlobalEnv {
            storage: Arc::new(storage),
        });
        let sql = "create table t (v1 int not null, v2 int not null); ";
        let mut stmts = SQLStatement::parse(sql).unwrap();
        stmts[0].bind(&mut binder).unwrap();
        let logical_planner = LogicalPlanGenerator::new();
        let physical_planner = PhysicalPlanGenerator::new();
        let logical_plan = logical_planner.generate_logical_plan(&stmts[0]).unwrap();
        let physical_plan = physical_planner
            .generate_physical_plan(&logical_plan)
            .unwrap();
        let executor_builder = ExecutorBuilder::new(global_env.clone());
        let executor = executor_builder.build(physical_plan).unwrap();
        futures::executor::block_on(executor).unwrap();

        let id = TableRefId {
            database_id: 0,
            schema_id: 0,
            table_id: 0,
        };
        assert_eq!(
            catalog.get_table_id(
                DEFAULT_DATABASE_NAME.into(),
                DEFAULT_SCHEMA_NAME.into(),
                "t".into()
            ),
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
