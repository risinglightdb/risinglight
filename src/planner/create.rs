use super::*;
use crate::catalog::ColumnCatalog;
use crate::parser::CreateTableStmt;
use crate::types::{ColumnId, DatabaseId, SchemaId, TableId};

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTablePlan {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub column_descs: Vec<ColumnCatalog>,
}
