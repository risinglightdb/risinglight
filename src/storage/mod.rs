
mod table;

pub use table::*;

use std::collections::HashMap;
use crate::types::{DatabaseId, ColumnId, SchemaId, TableId};


pub type TableMap = HashMap<TableId, TableRef>; 
pub type DatabaseMap = HashMap<SchemaId, TableMap>;

pub struct Storage {
    store: DatabaseMap
}

