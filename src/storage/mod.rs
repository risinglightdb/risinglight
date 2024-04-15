// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Traits and basic data structures for RisingLight's all storage engines.

mod memory;
use async_trait::async_trait;
pub use memory::InMemoryStorage;

mod secondary;
use risinglight_proto::rowset::block_statistics::BlockStatisticsType;
pub use secondary::{SecondaryStorage, StorageOptions as SecondaryStorageOptions};

mod error;
pub use error::{StorageError, StorageResult, TracedStorageError};
use serde::Serialize;

mod chunk;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

pub use chunk::*;

use crate::array::DataChunk;
use crate::catalog::{ColumnCatalog, ColumnId, SchemaId, TableRefId};
use crate::types::DataValue;

/// A reference to a storage engine.
pub type StorageRef = Arc<dyn Storage>;

/// A storage engine.
#[async_trait]
pub trait Storage: Sync + Send + 'static {
    async fn create_table(
        &self,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
        ordered_pk_ids: &[ColumnId],
    ) -> StorageResult<()>;

    async fn get_table(&self, table_id: TableRefId) -> StorageResult<TableRef>;

    async fn drop_table(&self, table_id: TableRefId) -> StorageResult<()>;

    async fn shutdown(&self) -> StorageResult<()>;

    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns true if the storage engine supports range filter scan.
    fn support_range_filter_scan(&self) -> bool {
        false
    }

    /// Returns true if scanned table is sorted by primary key.
    fn table_is_sorted_by_primary_key(&self) -> bool {
        false
    }
}

pub type TableRef = Arc<dyn Table>;

/// A table in the storage engine. [`Table`] is by default a reference to a table,
/// so you could clone it and manipulate in different threads as you like.
#[async_trait]
pub trait Table: Sync + Send + 'static {
    /// Get schema of the current table
    fn columns(&self) -> StorageResult<Arc<[ColumnCatalog]>>;

    /// Begin a read-write-only txn
    async fn write(&self) -> StorageResult<BoxTransaction>;

    /// Begin a read-only txn
    async fn read(&self) -> StorageResult<BoxTransaction>;

    /// Begin a txn that might delete or update rows
    async fn update(&self) -> StorageResult<BoxTransaction>;

    /// Get table id
    fn table_id(&self) -> TableRefId;

    /// Get primary key
    fn primary_key(&self) -> Vec<ColumnId>;
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
pub type RowHandler = i64;

pub type BoxTransaction = Box<dyn Transaction>;

/// Represents a transaction in storage engine.
///
/// Dropping a [`Transaction`] implicitly aborts it.
#[async_trait]
pub trait Transaction: Sync + Send + 'static {
    /// Scan one or multiple columns.
    async fn scan(
        &self,
        col_idx: &[StorageColumnRef],
        options: ScanOptions,
    ) -> StorageResult<BoxTxnIterator>;

    /// Append data to the table. Generally, `columns` should be in the same order as
    /// [`ColumnCatalog`] when constructing the [`Table`].
    async fn append(&mut self, columns: DataChunk) -> StorageResult<()>;

    /// Delete a batch of records.
    async fn delete(&mut self, ids: &[RowHandler]) -> StorageResult<()>;

    /// Commit a transaction.
    async fn commit(&mut self) -> StorageResult<()>;

    /// Get statistics, such as row count, distinct count, etc.
    fn get_stats(&self, _ty: &[(BlockStatisticsType, StorageColumnRef)]) -> Vec<DataValue> {
        vec![]
    }
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

pub type BoxTxnIterator = Box<dyn TxnIterator>;

/// An iterator over table in a transaction.
#[async_trait]
pub trait TxnIterator: Send {
    /// get next batch of elements
    async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>>;
}
