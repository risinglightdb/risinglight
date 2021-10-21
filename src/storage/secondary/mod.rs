//! Secondary storage engine for RisingLight

mod iterator;
pub use iterator::*;

mod row_handler;
pub use row_handler::*;

mod table;
pub use table::*;

mod transaction;
pub use transaction::*;

mod mem_rowset;
pub use mem_rowset::*;

use super::{Storage, StorageError, StorageResult};
use crate::catalog::{ColumnCatalog, RootCatalog, RootCatalogRef, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Secondary storage of RisingLight.
pub struct SecondaryStorage {
    /// Catalog of the database
    /// TODO(chi): persist catalog in Secondary
    catalog: RootCatalogRef,

    /// All tables in the storage engine
    tables: RwLock<HashMap<TableRefId, SecondaryTable>>,
}

impl Default for SecondaryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl SecondaryStorage {
    pub fn new() -> Self {
        Self {
            catalog: Arc::new(RootCatalog::new()),
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub fn catalog(&self) -> &RootCatalogRef {
        &self.catalog
    }
}

impl Storage for SecondaryStorage {
    type TransactionType = SecondaryTransaction;
    type TableType = SecondaryTable;

    // The following implementation is exactly the same as in-memory engine.

    fn create_table(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
    ) -> StorageResult<()> {
        let db = self
            .catalog
            .get_database_by_id(database_id)
            .ok_or(StorageError::NotFound("database", database_id))?;
        let schema = db
            .get_schema_by_id(schema_id)
            .ok_or(StorageError::NotFound("schema", schema_id))?;
        if schema.get_table_by_name(table_name).is_some() {
            return Err(StorageError::Duplicated("table", table_name.into()));
        }
        let table_id = schema
            .add_table(table_name.into(), column_descs.to_vec(), false)
            .map_err(|_| StorageError::Duplicated("table", table_name.into()))?;

        let id = TableRefId {
            database_id,
            schema_id,
            table_id,
        };
        let table = SecondaryTable::new(id, column_descs);
        self.tables.write().insert(id, table);
        Ok(())
    }

    fn get_table(&self, table_id: TableRefId) -> StorageResult<SecondaryTable> {
        let table = self
            .tables
            .read()
            .get(&table_id)
            .ok_or(StorageError::NotFound("table", table_id.table_id))?
            .clone();
        Ok(table)
    }

    fn drop_table(&self, table_id: TableRefId) -> StorageResult<()> {
        self.tables
            .write()
            .remove(&table_id)
            .ok_or(StorageError::NotFound("table", table_id.table_id))?;
        let db = self
            .catalog
            .get_database_by_id(table_id.database_id)
            .unwrap();
        let schema = db.get_schema_by_id(table_id.schema_id).unwrap();
        schema.delete_table(table_id.table_id);
        Ok(())
    }
}
