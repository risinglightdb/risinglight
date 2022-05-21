// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

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
use std::future::Future;
use std::sync::{Arc, Mutex};

use super::{Storage, StorageError, StorageResult, TracedStorageError};
use crate::catalog::{ColumnCatalog, RootCatalog, RootCatalogRef, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId};

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
        }
    }

    pub fn catalog(&self) -> &RootCatalogRef {
        &self.catalog
    }
}

impl Storage for InMemoryStorage {
    type CreateTableResultFuture<'a> = impl Future<Output = StorageResult<()>> + 'a;
    type DropTableResultFuture<'a> = impl Future<Output = StorageResult<()>> + 'a;
    type TransactionType = InMemoryTransaction;
    type TableType = InMemoryTable;

    fn create_table<'a>(
        &'a self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &'a str,
        column_descs: &'a [ColumnCatalog],
        ordered_pk_ids: &'a [ColumnId],
    ) -> Self::CreateTableResultFuture<'a> {
        async move {
            let db = self
                .catalog
                .get_database_by_id(database_id)
                .ok_or_else(|| TracedStorageError::not_found("database", database_id))?;
            let schema = db
                .get_schema_by_id(schema_id)
                .ok_or_else(|| TracedStorageError::not_found("schema", schema_id))?;
            if schema.get_table_by_name(table_name).is_some() {
                return Err(TracedStorageError::duplicated("table", table_name));
            }
            let ref_id = TableRefId::new(database_id, schema_id, 0);
            let table_id = self
                .catalog
                .add_table(
                    ref_id,
                    table_name.into(),
                    column_descs.to_vec(),
                    false,
                    ordered_pk_ids.to_vec(),
                )
                .map_err(|_| StorageError::Duplicated("table", table_name.into()))?;

            let id = TableRefId {
                database_id,
                schema_id,
                table_id,
            };
            let table = InMemoryTable::new(id, column_descs);
            self.tables.lock().unwrap().insert(id, table);
            Ok(())
        }
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

    fn drop_table(&self, table_id: TableRefId) -> Self::DropTableResultFuture<'_> {
        async move {
            self.tables
                .lock()
                .unwrap()
                .remove(&table_id)
                .ok_or_else(|| TracedStorageError::not_found("table", table_id.table_id))?;
            self.catalog.drop_table(table_id);
            Ok(())
        }
    }
}
