mod table;

pub use table::*;

use crate::catalog::{ColumnCatalog, RootCatalog, RootCatalogRef, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId, TableId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("failed to read table")]
    ReadTableError,
    #[error("failed to write table")]
    WriteTableError,
    #[error("failed to create table: {0}")]
    CreateTableError(String),
}

pub trait StorageManager: Sync + Send {
    fn create_table(
        &self,
        database_id: &DatabaseId,
        schema_id: &SchemaId,
        table_name: &String,
        column_descs: &[ColumnCatalog],
    ) -> Result<(), StorageError>;
    fn get_table(&self, table_id: &TableRefId) -> Result<TableRef, StorageError>;
    fn drop_table(&self, table_id: &TableRefId) -> Result<(), StorageError>;
}

pub type StorageManagerRef = Arc<dyn StorageManager>;

pub struct InMemoryStorageManager {
    catalog_ref: RootCatalogRef,
    tables: Mutex<HashMap<TableRefId, Table>>,
}

impl InMemoryStorageManager {
    pub fn new() -> Self {
        InMemoryStorageManager {
            catalog_ref: Arc::new(RootCatalog::new()),
            tables: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_catalog(&self) -> RootCatalogRef {
        self.catalog_ref.clone()
    }
}

impl StorageManager for InMemoryStorageManager {
    fn create_table(
        &self,
        database_id: &DatabaseId,
        schema_id: &SchemaId,
        table_name: &String,
        column_descs: &[ColumnCatalog],
    ) -> Result<(), StorageError> {
        let db = self
            .catalog_ref
            .get_database_by_id(*database_id)
            .ok_or_else(|| StorageError::CreateTableError("database not found".to_string()))?;
        let schema = db
            .get_schema_by_id(*schema_id)
            .ok_or_else(|| StorageError::CreateTableError("schema not found".to_string()))?;
        if schema.get_table_by_name(table_name).is_none() {
            if schema
                .add_table(table_name.clone(), column_descs.to_vec(), false)
                .is_err()
            {
                Err(StorageError::CreateTableError(
                    "duplicated table".to_string(),
                ))
            } else {
                Ok(())
            }
        } else {
            Err(StorageError::CreateTableError(
                "duplicated table".to_string(),
            ))
        }
    }

    fn get_table(&self, table_id: &TableRefId) -> Result<TableRef, StorageError> {
        Err(StorageError::ReadTableError)
    }
    fn drop_table(&self, table_id: &TableRefId) -> Result<(), StorageError> {
        Err(StorageError::WriteTableError)
    }
}
