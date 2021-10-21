use super::*;
use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::{ColumnDesc, TableRefId};
use crate::storage::Table;
use async_trait::async_trait;
use itertools::Itertools;
use std::sync::{Arc, RwLock};
use std::vec::Vec;

/// A table in in-memory engine. This struct can be freely cloned, as it
/// only serves as a reference to a table.
#[derive(Clone)]
pub struct InMemoryTable {
    pub(super) table_ref_id: TableRefId,
    pub(super) inner: InMemoryTableInnerRef,
}

pub(super) struct InMemoryTableInner {
    chunks: Vec<DataChunkRef>,
    columns: HashMap<ColumnId, ColumnDesc>,
}

pub(super) type InMemoryTableInnerRef = Arc<RwLock<InMemoryTableInner>>;

impl InMemoryTableInner {
    pub fn new(columns: &[ColumnCatalog]) -> Self {
        Self {
            chunks: vec![],
            columns: columns
                .iter()
                .map(|col| (col.id(), col.desc().clone()))
                .collect(),
        }
    }

    pub fn append(&mut self, chunk: DataChunk) -> Result<(), StorageError> {
        // The BaseTable will not validate the datachunk, it is Binder's and Executor's task.
        // TODO(runji): check and reorder columns
        self.chunks.push(Arc::new(chunk));
        Ok(())
    }

    pub fn get_all_chunks(&self) -> StorageResult<Vec<DataChunkRef>> {
        Ok(self.chunks.clone())
    }

    fn column_descs(&self, ids: &[ColumnId]) -> StorageResult<Vec<ColumnDesc>> {
        ids.iter()
            .map(|id| {
                self.columns
                    .get(id)
                    .cloned()
                    .ok_or(StorageError::InvalidColumn(*id))
            })
            .try_collect()
    }
}

impl InMemoryTable {
    pub fn new(table_ref_id: TableRefId, columns: &[ColumnCatalog]) -> Self {
        Self {
            table_ref_id,
            inner: Arc::new(RwLock::new(InMemoryTableInner::new(columns))),
        }
    }
}

#[async_trait]
impl Table for InMemoryTable {
    type TransactionType = InMemoryTransaction;

    fn column_descs(&self, ids: &[ColumnId]) -> StorageResult<Vec<ColumnDesc>> {
        let inner = self.inner.read().unwrap();
        inner.column_descs(ids)
    }

    fn table_id(&self) -> TableRefId {
        self.table_ref_id
    }

    async fn write(&self) -> StorageResult<Self::TransactionType> {
        Ok(InMemoryTransaction::start(self)?)
    }

    async fn read(&self) -> StorageResult<Self::TransactionType> {
        Ok(InMemoryTransaction::start(self)?)
    }

    async fn update(&self) -> StorageResult<Self::TransactionType> {
        Ok(InMemoryTransaction::start(self)?)
    }
}
