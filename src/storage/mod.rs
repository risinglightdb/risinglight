mod table;

pub use table::*;

use crate::types::{ColumnId, DatabaseId, SchemaId, TableId};
use std::collections::HashMap;

pub type TableMap = HashMap<TableId, TableRef>;
pub type DatabaseMap = HashMap<SchemaId, TableMap>;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("failed to read table")]
    ReadTableError,
    #[error("failed to write table")]
    WriteTableError,
}

pub struct Storage {
    store: DatabaseMap,
}
