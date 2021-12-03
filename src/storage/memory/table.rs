use super::*;
use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::TableRefId;
use crate::storage::Table;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::vec::Vec;

/// A table in in-memory engine. This struct can be freely cloned, as it
/// only serves as a reference to a table.
#[derive(Clone)]
pub struct InMemoryTable {
    pub(super) table_ref_id: TableRefId,
    pub(super) columns: Arc<[ColumnCatalog]>,
    pub(super) inner: InMemoryTableInnerRef,
}

pub(super) struct InMemoryTableInner {
    chunks: Vec<DataChunkRef>,
    deleted_rows: HashSet<usize>,
}

pub(super) type InMemoryTableInnerRef = Arc<RwLock<InMemoryTableInner>>;

impl InMemoryTableInner {
    pub fn new() -> Self {
        Self {
            chunks: vec![],
            deleted_rows: HashSet::new(),
        }
    }

    pub fn append(&mut self, chunk: DataChunk) -> Result<(), StorageError> {
        self.chunks.push(Arc::new(chunk));
        Ok(())
    }

    pub fn delete(&mut self, row_id: usize) -> Result<(), StorageError> {
        self.deleted_rows.insert(row_id);
        Ok(())
    }

    pub fn get_all_chunks(&self) -> Vec<DataChunkRef> {
        self.chunks.clone()
    }

    pub fn get_all_deleted_rows(&self) -> HashSet<usize> {
        self.deleted_rows.clone()
    }
}

impl InMemoryTable {
    pub fn new(table_ref_id: TableRefId, columns: &[ColumnCatalog]) -> Self {
        Self {
            table_ref_id,
            columns: columns.into(),
            inner: Arc::new(RwLock::new(InMemoryTableInner::new())),
        }
    }
}

#[async_trait]
impl Table for InMemoryTable {
    type TransactionType = InMemoryTransaction;

    fn columns(&self) -> StorageResult<Arc<[ColumnCatalog]>> {
        Ok(self.columns.clone())
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
