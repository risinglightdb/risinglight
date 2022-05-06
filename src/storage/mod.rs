// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Traits and basic data structures for RisingLight's all storage engines.

mod memory;
pub use memory::InMemoryStorage;

mod secondary;
pub use secondary::{SecondaryStorage, StorageOptions as SecondaryStorageOptions};

mod error;
pub use error::{StorageError, StorageResult, TracedStorageError};

mod chunk;
use std::future::Future;
use std::sync::Arc;

pub use chunk::*;
use enum_dispatch::enum_dispatch;

use crate::array::{ArrayImpl, DataChunk};
use crate::binder::BoundExpr;
use crate::catalog::{ColumnCatalog, TableRefId};
use crate::types::{ColumnId, DataValue, DatabaseId, SchemaId};

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

impl StorageImpl {
    pub fn enable_filter_scan(&self) -> bool {
        match self {
            Self::SecondaryStorage(_) => true,
            Self::InMemoryStorage(_) => false,
        }
    }
}

/// Represents a storage engine.
pub trait Storage: Sync + Send + 'static {
    /// the following two result future types to avoid `impl Future` return different types when
    /// impl `Storage`.
    type CreateTableResultFuture<'a>: Future<Output = StorageResult<()>> + Send + 'a
    where
        Self: 'a;
    type DropTableResultFuture<'a>: Future<Output = StorageResult<()>> + Send + 'a
    where
        Self: 'a;

    /// Type of the transaction.
    type TransactionType: Transaction;

    /// Type of the table belonging to this storage engine.
    type TableType: Table<TransactionType = Self::TransactionType>;

    fn create_table<'a>(
        &'a self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &'a str,
        column_descs: &'a [ColumnCatalog],
        ordered_pk_ids: &'a [ColumnId],
    ) -> Self::CreateTableResultFuture<'a>;

    fn get_table(&self, table_id: TableRefId) -> StorageResult<Self::TableType>;

    fn drop_table(&self, table_id: TableRefId) -> Self::DropTableResultFuture<'_>;
}

/// A table in the storage engine. [`Table`] is by default a reference to a table,
/// so you could clone it and manipulate in different threads as you like.
pub trait Table: Sync + Send + Clone + 'static {
    /// Type of the transaction.
    type TransactionType: Transaction;

    type WriteResultFuture<'a>: Future<Output = StorageResult<Self::TransactionType>> + Send + 'a
    where
        Self: 'a;
    type ReadResultFuture<'a>: Future<Output = StorageResult<Self::TransactionType>> + Send + 'a
    where
        Self: 'a;
    type UpdateResultFuture<'a>: Future<Output = StorageResult<Self::TransactionType>> + Send + 'a
    where
        Self: 'a;

    /// Get schema of the current table
    fn columns(&self) -> StorageResult<Arc<[ColumnCatalog]>>;

    /// Begin a read-write-only txn
    fn write(&self) -> Self::WriteResultFuture<'_>;

    /// Begin a read-only txn
    fn read(&self) -> Self::ReadResultFuture<'_>;

    /// Begin a txn that might delete or update rows
    fn update(&self) -> Self::UpdateResultFuture<'_>;

    /// Get table id
    fn table_id(&self) -> TableRefId;
}

/// Reference to a column.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum StorageColumnRef {
    /// A runtime column which contains necessary information to locate a row
    /// **only valid in the current transaction**.
    RowHandler,
    /// User column index. Note that this index is NOT the `ColumnId` in catalog. It is the storage
    /// column id, which is the same as the position of a column in the column catalog passed to a
    /// RowSet.
    Idx(u32),
}

/// A temporary reference to a row in table.
pub trait RowHandler: Sync + Send + 'static {
    fn from_column(column: &ArrayImpl, idx: usize) -> Self;
}

/// Represents a transaction in storage engine.
pub trait Transaction: Sync + Send + 'static {
    /// Type of the table iterator
    type TxnIteratorType: TxnIterator;

    /// Type of the unique reference to a row
    type RowHandlerType: RowHandler;

    type ScanResultFuture<'a>: Future<Output = StorageResult<Self::TxnIteratorType>> + Send + 'a
    where
        Self: 'a;
    type AppendResultFuture<'a>: Future<Output = StorageResult<()>> + Send + 'a
    where
        Self: 'a;
    type DeleteResultFuture<'a>: Future<Output = StorageResult<()>> + Send + 'a
    where
        Self: 'a;
    type CommitResultFuture<'a>: Future<Output = StorageResult<()>> + Send + 'a
    where
        Self: 'a;
    type AbortResultFuture<'a>: Future<Output = StorageResult<()>> + Send + 'a
    where
        Self: 'a;
    /// Scan one or multiple columns.
    fn scan<'a>(
        &'a self,
        begin_sort_key: &'a [DataValue],
        end_sort_key: &'a [DataValue],
        col_idx: &'a [StorageColumnRef],
        is_sorted: bool,
        reversed: bool,
        expr: Option<BoundExpr>,
    ) -> Self::ScanResultFuture<'a>;

    /// Append data to the table. Generally, `columns` should be in the same order as
    /// [`ColumnCatalog`] when constructing the [`Table`].
    fn append(&mut self, columns: DataChunk) -> Self::AppendResultFuture<'_>;

    /// Delete a record.
    fn delete<'a>(&'a mut self, id: &'a Self::RowHandlerType) -> Self::DeleteResultFuture<'a>;

    /// Commit a transaction.
    fn commit<'a>(self) -> Self::CommitResultFuture<'a>;

    /// Abort a transaction.
    fn abort<'a>(self) -> Self::AbortResultFuture<'a>;
}

/// An iterator over table in a transaction.
pub trait TxnIterator: Send {
    type NextFuture<'a>: Future<Output = StorageResult<Option<DataChunk>>> + Send + 'a
    where
        Self: 'a;

    /// get next batch of elements
    fn next_batch(&mut self, expected_size: Option<usize>) -> Self::NextFuture<'_>;
}
