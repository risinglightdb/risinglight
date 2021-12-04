use super::*;
use crate::{catalog::TableRefId, physical_planner::PhysicalCreateTable, storage::StorageRef};

/// The executor of `CREATE TABLE` statement.
pub struct CreateTableExecutor {
    pub plan: PhysicalCreateTable,
    pub catalog: RootCatalogRef,
    pub storage: StorageRef,
}

impl Executor for CreateTableExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let db = self.catalog.get_database(self.plan.database_id).unwrap();
        let schema = db.get_schema(self.plan.schema_id).unwrap();
        let table_id = schema.add_table(&self.plan.table_name).unwrap();
        let table = schema.get_table(table_id).unwrap();
        for (name, desc) in &self.plan.columns {
            table.add_column(name, desc.clone()).unwrap();
        }
        self.storage.add_table(TableRefId::new(
            self.plan.database_id,
            self.plan.schema_id,
            table_id,
        ))?;
        Ok(DataChunk::single(1))
    }
}
