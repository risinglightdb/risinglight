//! Traits and basic data structures for RisingLight's all storage engines.

mod memory;
pub use memory::InMemoryStorage;

mod secondary;
pub use secondary::{SecondaryStorage, StorageOptions as SecondaryStorageOptions};

mod error;
pub use error::{StorageError, StorageResult};

mod chunk;
pub use chunk::*;

use crate::array::{ArrayImpl, DataChunk};
use crate::catalog::{ColumnCatalog, ColumnDesc, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId};

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

use std::sync::Arc;

#[enum_dispatch(StorageDispatch)]
#[derive(Clone)]
pub enum StorageImpl {
    InMemoryStorage(Arc<InMemoryStorage>),
    SecondaryStorage(Arc<SecondaryStorage>),
}

/// A trait for implementing `From` and `Into` [`StorageImpl`] with `enum_dispatch`.
#[enum_dispatch]
pub trait StorageDispatch {}

impl<S: Storage> StorageDispatch for S {}

#[cfg(test)]
impl StorageImpl {
    pub fn as_in_memory_storage(&self) -> Arc<InMemoryStorage> {
        self.clone().try_into().unwrap()
    }
}

/// Represents a storage engine.
pub trait Storage: Sync + Send + 'static {
    /// Type of the transaction.
    type TransactionType: Transaction;

    /// Type of the table belonging to this storage engine.
    type TableType: Table<TransactionType = Self::TransactionType>;

    fn create_table(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
    ) -> StorageResult<()>;
    fn get_table(&self, table_id: TableRefId) -> StorageResult<Self::TableType>;
    fn drop_table(&self, table_id: TableRefId) -> StorageResult<()>;
}

/// A table in the storage engine. [`Table`] is by default a reference to a table,
/// so you could clone it and manipulate in different threads as you like.
#[async_trait]
pub trait Table: Sync + Send + Clone + 'static {
    /// Type of the transaction.
    type TransactionType: Transaction;

    /// Get schema of the current table
    fn column_descs(&self, ids: &[ColumnId]) -> StorageResult<Vec<ColumnDesc>>;

    /// Begin a read-write-only txn
    async fn write(&self) -> StorageResult<Self::TransactionType>;

    /// Begin a read-only txn
    async fn read(&self) -> StorageResult<Self::TransactionType>;

    /// Begin a txn that might delete or update rows
    async fn update(&self) -> StorageResult<Self::TransactionType>;

    /// Get table id
    fn table_id(&self) -> TableRefId;
}

/// Reference to a column.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum StorageColumnRef {
    /// A runtime column which contains necessary information to locate a row
    /// **only valid in the current transaction**.
    RowHandler,
    /// User column.
    Idx(u32),
}

/// A temporary reference to a row in table.
pub trait RowHandler: Sync + Send + 'static {
    fn from_column(column: &ArrayImpl, idx: usize) -> Self;
}

/// Represents a transaction in storage engine.
#[async_trait]
pub trait Transaction: Sync + Send + 'static {
    /// Type of the table iterator
    type TxnIteratorType: TxnIterator;

    /// Type of the unique reference to a row
    type RowHandlerType: RowHandler;

    /// Scan one or multiple columns.
    async fn scan(
        &self,
        begin_sort_key: Option<&[u8]>,
        end_sort_key: Option<&[u8]>,
        col_idx: &[StorageColumnRef],
        reversed: bool,
    ) -> StorageResult<Self::TxnIteratorType>;

    /// Append data to the table. Generally, `columns` should be in the same order as
    /// [`ColumnCatalog`] when constructing the [`Table`].
    async fn append(&mut self, columns: DataChunk) -> StorageResult<()>;

    /// Delete a record.
    async fn delete(&mut self, id: &Self::RowHandlerType) -> StorageResult<()>;

    /// Commit a transaction.
    async fn commit(self) -> StorageResult<()>;

    /// Abort a transaction.
    async fn abort(self) -> StorageResult<()>;
}

/// An iterator over table in a transaction.
#[async_trait]
pub trait TxnIterator: Send {
    /// get next batch of elements
    async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>>;
}
