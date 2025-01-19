// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! In-memory storage implementation of RisingLight.
//!
//! RisingLight's in-memory representation of data is very simple. Currently,
//! it is simple a vector of `DataChunk`. Upon insertion, users' data are
//! simply appended to the end of the vector.
//!
//! The in-memory engine provides snapshot isolation. In the current implementation,
//! a snapshot (clone of all `DataChunk` references) will be created upon a
//! transaction starts. Inside transaction, we buffer all writes until commit.
//! We do not guarantee read-after-write consistency.
//!
//! Things not supported for now:
//! * deletion
//! * sort-key based scan
//! * reverse scan
//! * `RowHandler` scan

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::index::InMemoryIndexes;
use super::{InMemoryIndex, Storage, StorageError, StorageResult, TracedStorageError};
use crate::binder::IndexType;
use crate::catalog::{
    ColumnCatalog, ColumnId, IndexId, RootCatalog, RootCatalogRef, SchemaId, TableId, TableRefId,
};

mod table;
pub use table::InMemoryTable;

mod transaction;
pub use transaction::InMemoryTransaction;

mod iterator;
pub use iterator::InMemoryTxnIterator;

mod row_handler;
pub use row_handler::InMemoryRowHandler;

/// In-memory storage of RisingLight.
pub struct InMemoryStorage {
    catalog: RootCatalogRef,
    tables: Mutex<HashMap<TableRefId, InMemoryTable>>,
    indexes: Mutex<InMemoryIndexes>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            catalog: Arc::new(RootCatalog::new()),
            tables: Mutex::new(HashMap::new()),
            indexes: Mutex::new(InMemoryIndexes::new()),
        }
    }

    pub fn catalog(&self) -> &RootCatalogRef {
        &self.catalog
    }
}

impl Storage for InMemoryStorage {
    type Transaction = InMemoryTransaction;
    type Table = InMemoryTable;

    async fn create_table(
        &self,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
        ordered_pk_ids: &[ColumnId],
    ) -> StorageResult<()> {
        let schema = self
            .catalog
            .get_schema_by_id(schema_id)
            .ok_or_else(|| TracedStorageError::not_found("schema", schema_id))?;
        if schema.get_table_by_name(table_name).is_some() {
            return Err(TracedStorageError::duplicated("table", table_name));
        }
        let table_id = self
            .catalog
            .add_table(
                schema_id,
                table_name.into(),
                column_descs.to_vec(),
                ordered_pk_ids.to_vec(),
            )
            .map_err(|_| StorageError::Duplicated("table", table_name.into()))?;

        let id = TableRefId {
            schema_id,
            table_id,
        };
        let table = InMemoryTable::new(id, column_descs);
        self.tables.lock().unwrap().insert(id, table);
        Ok(())
    }

    fn get_table(&self, table_id: TableRefId) -> StorageResult<InMemoryTable> {
        let table = self
            .tables
            .lock()
            .unwrap()
            .get(&table_id)
            .ok_or_else(|| TracedStorageError::not_found("table", table_id.table_id))?
            .clone();
        Ok(table)
    }

    async fn drop_table(&self, table_id: TableRefId) -> StorageResult<()> {
        self.tables
            .lock()
            .unwrap()
            .remove(&table_id)
            .ok_or_else(|| TracedStorageError::not_found("table", table_id.table_id))?;
        self.catalog.drop_table(table_id);
        Ok(())
    }

    fn as_disk(&self) -> Option<&super::SecondaryStorage> {
        None
    }

    async fn create_index(
        &self,
        schema_id: SchemaId,
        index_name: &str,
        table_id: TableId,
        column_idxs: &[ColumnId],
        index_type: &IndexType,
    ) -> StorageResult<IndexId> {
        let idx_id = self
            .catalog
            .add_index(
                schema_id,
                index_name.to_string(),
                table_id,
                column_idxs,
                index_type,
            )
            .map_err(|_| StorageError::Duplicated("index", index_name.into()))?;
        self.indexes
            .lock()
            .unwrap()
            .add_index(schema_id, idx_id, table_id, column_idxs);
        // TODO: populate the index
        Ok(idx_id)
    }

    async fn get_index(
        &self,
        schema_id: SchemaId,
        index_id: IndexId,
    ) -> StorageResult<Arc<dyn InMemoryIndex>> {
        let idx = self
            .indexes
            .lock()
            .unwrap()
            .get_index(schema_id, index_id)
            .ok_or_else(|| StorageError::NotFound("index", index_id.to_string()))?;
        Ok(idx)
    }

    fn get_catalog(&self) -> Arc<RootCatalog> {
        self.catalog.clone()
    }
}
