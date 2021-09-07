mod table;

pub use table::*;

use crate::types::{ColumnId, DatabaseId, SchemaId, TableId};
use std::collections::HashMap;

pub type TableMap = HashMap<TableId, TableRef>;
pub type DatabaseMap = HashMap<SchemaId, TableMap>;

pub struct Storage {
    store: DatabaseMap,
}
