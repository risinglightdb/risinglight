use super::*;
use crate::array::DataChunk;
use crate::catalog::{ColumnId, TableRefId};

/// The executor of sequential scan operation.
pub struct SeqScanExecutor {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub storage: StorageRef,
}

impl Executor for SeqScanExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let table = self.storage.get_table(self.table_ref_id)?;
        let chunks = table.all_chunks()?;
        let chunk = DataChunk::concat(chunks.as_slice());
        Ok(chunk)
    }
}
