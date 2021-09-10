mod table;

pub use table::*;

use crate::catalog::{ColumnCatalog, RootCatalog, TableRefId};
use crate::types::{ColumnId, DatabaseId, SchemaId, TableId};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("failed to read table")]
    ReadTableError,
    #[error("failed to write table")]
    WriteTableError,
}

pub trait StorageManager: Sync + Send {
    fn create_table(
        &self,
        database_id: &DatabaseId,
        schema_id: &SchemaId,
        table_name: &String,
        column_descs: &Vec<ColumnCatalog>,
    ) -> Result<(), StorageError>;
    fn get_table(&self, table_id: &TableRefId) -> Result<TableRef, StorageError>;
    fn drop_table(&self, table_id: &TableRefId) -> Result<(), StorageError>;
}

pub struct InMemoryStorageManager {
    catalog_ref: Mutex<RootCatalog>,
    tables: Mutex<HashMap<TableRefId, Table>>,
}
