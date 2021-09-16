mod table;

pub use self::table::*;

use crate::catalog::{ColumnCatalog, RootCatalog, RootCatalogRef, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("failed to read table")]
    ReadTableError,
    #[error("failed to write table")]
    WriteTableError,
    #[error("{0}({1}) not found")]
    NotFound(&'static str, u32),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
    #[error("invalid column id: {0}")]
    InvalidColumn(ColumnId),
}

pub trait Storage: Sync + Send {
    fn create_table(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
    ) -> Result<(), StorageError>;
    fn get_table(&self, table_id: TableRefId) -> Result<TableRef, StorageError>;
    fn drop_table(&self, table_id: TableRefId) -> Result<(), StorageError>;
}

pub type StorageRef = Arc<dyn Storage>;

pub struct InMemoryStorage {
    catalog: RootCatalogRef,
    tables: Mutex<HashMap<TableRefId, TableRef>>,
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
    fn create_table(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: &str,
        column_descs: &[ColumnCatalog],
    ) -> Result<(), StorageError> {
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
        let table = BaseTable::new(id, column_descs);
        self.tables.lock().unwrap().insert(id, Arc::new(table));
        Ok(())
    }

    fn get_table(&self, table_id: TableRefId) -> Result<TableRef, StorageError> {
        let table = self
            .tables
            .lock()
            .unwrap()
            .get(&table_id)
            .ok_or(StorageError::NotFound("table", table_id.table_id))?
            .clone();
        Ok(table)
    }

    fn drop_table(&self, table_id: TableRefId) -> Result<(), StorageError> {
        self.tables.lock().unwrap().remove(&table_id);
        todo!("remove table from catalog");
        Ok(())
    }
}
