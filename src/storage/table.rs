use super::*;
use crate::array::DataChunkRef;
use crate::catalog::ColumnDesc;
use crate::catalog::TableRefId;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::vec::Vec;

pub enum Table {
    BaseTable,
    MaterializedView,
}

pub type TableRef = Arc<BaseTable>;

pub struct BaseTableInner {
    column_descs: Vec<ColumnDesc>,
    chunks: Vec<DataChunkRef>,
}

impl BaseTableInner {
    fn new(column_descs: &Vec<ColumnDesc>) -> BaseTableInner {
        BaseTableInner {
            column_descs: column_descs.to_vec(),
            chunks: vec![],
        }
    }
}

pub struct BaseTable {
    table_ref_id: TableRefId,
    inner: RwLock<BaseTableInner>,
}

impl BaseTable {
    pub fn new(table_ref_id: &TableRefId, column_descs: &Vec<ColumnDesc>) -> BaseTable {
        BaseTable {
            table_ref_id: *table_ref_id,
            inner: RwLock::new(BaseTableInner::new(column_descs)),
        }
    }

    // The BaseTable will not validate the datachunk, it is Binder's and Executor's task.
    pub fn append_datachunk(&mut self, chunk: DataChunkRef) -> Result<(), StorageError> {
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
