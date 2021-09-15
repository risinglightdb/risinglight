use super::*;
use crate::array::DataChunkRef;
use crate::catalog::TableRefId;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::vec::Vec;

pub enum Table {
    BaseTable(BaseTable),
    MaterializedView,
}

pub type TableRef = Arc<BaseTable>;

pub struct BaseTableInner {
    chunks: Vec<DataChunkRef>,
}

impl BaseTableInner {
    fn new() -> BaseTableInner {
        BaseTableInner { chunks: vec![] }
    }
}

pub struct BaseTable {
    table_ref_id: TableRefId,
    inner: RwLock<BaseTableInner>,
}

impl BaseTable {
    pub fn new(table_ref_id: TableRefId) -> BaseTable {
        BaseTable {
            table_ref_id,
            inner: RwLock::new(BaseTableInner::new()),
        }
    }

    // The BaseTable will not validate the datachunk, it is Binder's and Executor's task.
    pub fn append(&mut self, chunk: DataChunkRef) -> Result<(), StorageError> {
        let mut writer = self.get_writer()?;
        writer.chunks.push(chunk);
        Ok(())
    }

    pub fn get_all_chunks(&self) -> Result<Vec<DataChunkRef>, StorageError> {
        let reader = self.get_reader()?;
        Ok(reader.chunks.clone())
    }

    pub fn get_reader(&self) -> Result<RwLockReadGuard<BaseTableInner>, StorageError> {
        self.inner.read().map_err(|e| StorageError::ReadTableError)
    }

    pub fn get_writer(&self) -> Result<RwLockWriteGuard<BaseTableInner>, StorageError> {
        self.inner
            .write()
            .map_err(|e| StorageError::WriteTableError)
    }
}
