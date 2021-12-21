use super::*;
use crate::binder::BoundCreateTable;
use crate::catalog::TableRefId;
use crate::storage::StorageRef;

/// The executor of `CREATE TABLE` statement.
pub struct CreateTableExecutor {
    pub stmt: BoundCreateTable,
    pub catalog: CatalogRef,
    pub storage: StorageRef,
}

impl Executor for CreateTableExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let schema = self.catalog.get_schema(self.stmt.schema_id).unwrap();
        let table_id = schema.add_table(&self.stmt.table_name).unwrap();
        let table = schema.get_table(table_id).unwrap();
        for (name, desc) in &self.stmt.columns {
            table.add_column(name, desc.clone()).unwrap();
        }
        self.storage
            .add_table(TableRefId::new(self.stmt.schema_id, table_id))?;
        Ok(DataChunk::single(1))
    }
}
