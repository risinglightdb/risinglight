//! In-memory storage.
//!
//! RisingLight's in-memory representation of data is very simple. Currently,
//! it is simple a vector of `DataChunk`. Upon insertion, users' data are
//! simply appended to the end of the vector.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::array::DataChunk;
use crate::catalog::TableRefId;

/// The error type of storage operations.
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("table not found: {0:?}")]
    NotFound(TableRefId),
}

/// A specialized `Result` type for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

pub type StorageRef = Arc<InMemoryStorage>;
pub type InMemoryTableRef = Arc<InMemoryTable>;

/// In-memory storage.
pub struct InMemoryStorage {
    tables: Mutex<HashMap<TableRefId, InMemoryTableRef>>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    /// Create a new in-memory storage.
    pub fn new() -> Self {
        InMemoryStorage {
            tables: Mutex::new(HashMap::new()),
        }
    }

    /// Add a table.
    pub fn add_table(&self, id: TableRefId) -> StorageResult<()> {
        let table = Arc::new(InMemoryTable::new(id));
        self.tables.lock().unwrap().insert(id, table);
        Ok(())
    }

    /// Get a table.
    pub fn get_table(&self, id: TableRefId) -> StorageResult<InMemoryTableRef> {
        self.tables
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(StorageError::NotFound(id))
    }
}

/// A table in in-memory engine.
pub struct InMemoryTable {
    #[allow(dead_code)]
    id: TableRefId,
    inner: RwLock<InMemoryTableInner>,
}

#[derive(Default)]
struct InMemoryTableInner {
    chunks: Vec<DataChunk>,
}

impl InMemoryTable {
    fn new(id: TableRefId) -> Self {
        Self {
            id,
            inner: RwLock::new(InMemoryTableInner::default()),
        }
    }

    /// Append a chunk to the table.
    pub fn append(&self, chunk: DataChunk) -> StorageResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.chunks.push(chunk);
        Ok(())
    }

    /// Get all chunks of the table.
    pub fn all_chunks(&self) -> StorageResult<Vec<DataChunk>> {
        let inner = self.inner.read().unwrap();
        Ok(inner.chunks.clone())
    }
}
