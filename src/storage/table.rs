use super::*;
use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::{ColumnDesc, TableRefId};
use std::sync::{Arc, RwLock};
use std::vec::Vec;

pub enum Table {
    BaseTable(BaseTable),
    MaterializedView,
}

pub type TableRef = Arc<BaseTable>;

pub struct BaseTable {
    table_ref_id: TableRefId,
    inner: RwLock<BaseTableInner>,
}

#[derive(Default)]
struct BaseTableInner {
    chunks: Vec<DataChunkRef>,
    columns: HashMap<ColumnId, ColumnDesc>,
}

impl BaseTable {
    pub fn new(table_ref_id: TableRefId, columns: &[ColumnCatalog]) -> BaseTable {
        BaseTable {
            table_ref_id,
            inner: RwLock::new(BaseTableInner {
                chunks: vec![],
                columns: columns
                    .iter()
                    .map(|col| (col.id(), col.desc().clone()))
                    .collect(),
            }),
        }
    }

    // The BaseTable will not validate the datachunk, it is Binder's and Executor's task.
    pub fn append(&self, chunk: DataChunk) -> Result<(), StorageError> {
        let mut inner = self.inner.write().unwrap();
        // TODO(runji): check and reorder columns
        inner.chunks.push(Arc::new(chunk));
        Ok(())
    }

    pub fn get_all_chunks(&self) -> Result<Vec<DataChunkRef>, StorageError> {
        let inner = self.inner.read().unwrap();
        Ok(inner.chunks.clone())
    }

    pub fn column_descs(&self, ids: &[ColumnId]) -> Result<Vec<ColumnDesc>, StorageError> {
        let inner = self.inner.read().unwrap();
        let mut ret = vec![];
        for id in ids {
            ret.push(
                inner
                    .columns
                    .get(id)
                    .ok_or(StorageError::InvalidColumn(*id))?
                    .clone(),
            );
        }
        Ok(ret)
    }
}
