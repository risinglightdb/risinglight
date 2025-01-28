// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Traits and basic data structures for RisingLight's all storage engines.

mod memory;
pub use memory::InMemoryStorage;

mod secondary;
pub use secondary::{SecondaryStorage, StorageOptions as SecondaryStorageOptions};

mod index;
pub use index::InMemoryIndex;

mod error;
pub use error::{StorageError, StorageResult, TracedStorageError};
use serde::Serialize;

mod chunk;
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

pub use chunk::*;
use enum_dispatch::enum_dispatch;

use crate::array::{ArrayImpl, DataChunk};
use crate::binder::IndexType;
use crate::catalog::{
    ColumnCatalog, ColumnId, IndexId, RootCatalog, SchemaId, TableId, TableRefId,
};
use crate::types::DataValue;

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
    /// Returns true if the storage engine supports range filter scan.
    pub fn support_range_filter_scan(&self) -> bool {
        match self {
            Self::SecondaryStorage(_) => true,
            Self::InMemoryStorage(_) => false,
        }
    }

    /// Returns true if scanned table is sorted by primary key.
    pub fn table_is_sorted_by_primary_key(&self) -> bool {
        match self {
            Self::SecondaryStorage(_) => true,
            Self::InMemoryStorage(_) => false,
        }
    }
}

/// Represents a storage engine.
pub trait Storage: Sync + Send + 'static {
    /// Type of the transaction.
    type Transaction: Transaction;

    /// Type of the table belonging to this storage engine.
    type Table: Table<Transaction = Self::Transaction>;

    fn create_table(
        &self,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
        ordered_pk_ids: &[ColumnId],
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn get_table(&self, table_id: TableRefId) -> StorageResult<Self::Table>;

    fn drop_table(&self, table_id: TableRefId) -> impl Future<Output = StorageResult<()>> + Send;

    fn create_index(
        &self,
        schema_id: SchemaId,
        index_name: &str,
        table_id: TableId,
        column_idxs: &[ColumnId],
        index_type: &IndexType,
    ) -> impl Future<Output = StorageResult<IndexId>> + Send;

    /// Get the catalog of the storage engine.
    ///
    /// TODO: users should not be able to modify the catalog.
    fn get_catalog(&self) -> Arc<RootCatalog>;

    fn get_index(
        &self,
        schema_id: SchemaId,
        index_id: IndexId,
    ) -> impl Future<Output = StorageResult<Arc<dyn InMemoryIndex>>> + Send;

    // XXX: remove this
    fn as_disk(&self) -> Option<&SecondaryStorage>;
}

/// A table in the storage engine. [`Table`] is by default a reference to a table,
/// so you could clone it and manipulate in different threads as you like.
pub trait Table: Sync + Send + Clone + 'static {
    /// Type of the transaction.
    type Transaction: Transaction;

    /// Get schema of the current table
    fn columns(&self) -> StorageResult<Arc<[ColumnCatalog]>>;

    /// Begin a read-write-only txn
    fn write(&self) -> impl Future<Output = StorageResult<Self::Transaction>> + Send + '_;

    /// Begin a read-only txn
    fn read(&self) -> impl Future<Output = StorageResult<Self::Transaction>> + Send + '_;

    /// Begin a txn that might delete or update rows
    fn update(&self) -> impl Future<Output = StorageResult<Self::Transaction>> + Send + '_;

    /// Get table id
    fn table_id(&self) -> TableRefId;

    /// Get primary key
    fn ordered_pk_ids(&self) -> Vec<ColumnId>;
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
///
/// Dropping a [`Transaction`] implicitly aborts it.
pub trait Transaction: Sync + Send + 'static {
    /// Type of the table iterator
    type TxnIteratorType: TxnIterator;

    /// Type of the unique reference to a row
    type RowHandlerType: RowHandler;

    /// Scan one or multiple columns.
    fn scan(
        &self,
        col_idx: &[StorageColumnRef],
        options: ScanOptions,
    ) -> impl Future<Output = StorageResult<Self::TxnIteratorType>> + Send;

    /// Append data to the table. Generally, `columns` should be in the same order as
    /// [`ColumnCatalog`] when constructing the [`Table`].
    fn append(&mut self, columns: DataChunk) -> impl Future<Output = StorageResult<()>> + Send;

    /// Delete a record.
    fn delete(
        &mut self,
        id: &Self::RowHandlerType,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Commit a transaction.
    fn commit(self) -> impl Future<Output = StorageResult<()>> + Send;

    /// Abort a transaction.
    fn abort(self) -> impl Future<Output = StorageResult<()>> + Send;
}

/// Options for scanning.
#[derive(Debug, Default)]
pub struct ScanOptions {
    is_sorted: bool,
    reversed: bool,
    filter: Option<KeyRange>,
}

impl ScanOptions {
    /// Scan with filter.
    pub fn with_filter_opt(mut self, filter: Option<KeyRange>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_sorted(mut self, sorted: bool) -> Self {
        self.is_sorted = sorted;
        self
    }
}

/// A range of keys.
///
/// # Example
/// ```text
/// // key > 1
/// KeyRange {
///     start: Bound::Excluded(DataValue::Int64(Some(1))),
///     end: Bound::Unbounded,
/// }
///
/// // key = 0
/// KeyRange {
///     start: Bound::Included(DataValue::Int64(Some(0))),
///     end: Bound::Included(DataValue::Int64(Some(0))),
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct KeyRange {
    /// Start bound.
    pub start: Bound<DataValue>,
    /// End bound.
    pub end: Bound<DataValue>,
}

impl RangeBounds<DataValue> for KeyRange {
    fn start_bound(&self) -> Bound<&DataValue> {
        match &self.start {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
        }
    }

    fn end_bound(&self) -> Bound<&DataValue> {
        match &self.end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
        }
    }
}

/// An iterator over table in a transaction.
pub trait TxnIterator: Send {
    /// get next batch of elements
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> impl Future<Output = StorageResult<Option<DataChunk>>> + Send + '_;
}
