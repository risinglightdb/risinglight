use crate::catalog::ColumnCatalog;

use crate::types::{DatabaseId, SchemaId};

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTablePhysicalPlan {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub column_descs: Vec<ColumnCatalog>,
}
