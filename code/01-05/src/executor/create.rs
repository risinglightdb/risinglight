use super::*;
use crate::{binder::BoundCreateTable, catalog::TableRefId, storage::StorageRef};

/// The executor of `CREATE TABLE` statement.
pub struct CreateTableExecutor {
    pub stmt: BoundCreateTable,
    pub catalog: RootCatalogRef,
    pub storage: StorageRef,
}

impl Executor for CreateTableExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let db = self.catalog.get_database(self.stmt.database_id).unwrap();
        let schema = db.get_schema(self.stmt.schema_id).unwrap();
        let table_id = schema.add_table(&self.stmt.table_name).unwrap();
        let table = schema.get_table(table_id).unwrap();
        for (name, desc) in &self.stmt.columns {
            table.add_column(name, desc.clone()).unwrap();
        }
        self.storage.add_table(TableRefId::new(
            self.stmt.database_id,
            self.stmt.schema_id,
            table_id,
        ))?;
        Ok(DataChunk::single(1))
    }
}
