use super::*;
use crate::array::DataChunk;
use crate::catalog::{ColumnId, TableRefId};
use crate::storage::StorageRef;

/// The executor of `INSERT` statement.
pub struct InsertExecutor {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub storage: StorageRef,
    pub child: BoxedExecutor,
}

impl Executor for InsertExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let table = self.storage.get_table(self.table_ref_id)?;
        let chunk = self.child.execute()?;
        let cnt = chunk.cardinality();
        table.append(chunk)?;
        Ok(DataChunk::single(cnt as i32))
    }
}
