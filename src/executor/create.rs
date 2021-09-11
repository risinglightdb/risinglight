use super::*;
use crate::catalog::ColumnCatalog;
use crate::storage::StorageManagerRef;
use crate::types::{DatabaseId, SchemaId};

pub struct CreateTableExecutor {
    storage_ref: StorageManagerRef,
    database_id: DatabaseId,
    schema_id: SchemaId,
    table_name: String,
    column_catalogs: Vec<ColumnCatalog>,
}

impl CreateTableExecutor {
    pub fn new(
        storage_ref: StorageManagerRef,
        database_id: &DatabaseId,
        schema_id: &SchemaId,
        table_name: &String,
        column_catalogs: &[ColumnCatalog],
    ) -> Self {
        CreateTableExecutor {
            storage_ref: storage_ref,
            database_id: *database_id,
            schema_id: *schema_id,
            table_name: table_name.clone(),
            column_catalogs: column_catalogs.to_vec(),
        }
    }
}

impl Executor for CreateTableExecutor {
    fn init(&mut self) -> Result<(), ExecutorError> {
        Ok(())
    }
    fn execute(&mut self, chunk: ExecutionResult) -> Result<ExecutionResult, ExecutorError> {
        if self
            .storage_ref
            .create_table(
                &self.database_id,
                &self.schema_id,
                &self.table_name,
                &self.column_catalogs,
            )
            .is_err()
        {
            Err(ExecutorError::CreateTableError)
        } else {
            Ok(ExecutionResult::Done)
        }
    }
    fn finish(&mut self) -> Result<(), ExecutorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::{Bind, Binder};
    use crate::catalog::{
        ColumnCatalog, ColumnDesc, TableRefId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };
    use crate::executor::{ExecutionResult, ExecutorBuilder};
    use crate::logical_plan::LogicalPlanGenerator;
    use crate::parser::SQLStatement;
    use crate::physical_plan::PhysicalPlanGenerator;
    use crate::server::GlobalVariables;
    use crate::storage::InMemoryStorageManager;
    use crate::types::{DataType, DataTypeKind};
    use std::sync::Arc;
    #[test]
    fn test_create() {
        let storage_mgr = InMemoryStorageManager::new();
        let catalog = storage_mgr.get_catalog();
        let mut binder = Binder::new(storage_mgr.get_catalog());
        let global_env = Arc::new(GlobalVariables {
            storage_mgr_ref: Arc::new(storage_mgr),
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
        let executor_builder = ExecutorBuilder::new(&physical_plan, global_env);
        let mut executors = executor_builder.build_plan().unwrap();
        executors.init().unwrap();
        executors.execute(ExecutionResult::Done).unwrap();
        executors.finish().unwrap();
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
            ColumnCatalog::new(
                0,
                "v1".into(),
                ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
            )
        );
        assert_eq!(
            col1,
            ColumnCatalog::new(
                1,
                "v2".into(),
                ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
            )
        );
    }
}
