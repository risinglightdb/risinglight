use super::*;
use crate::binder::BoundCreateTable;
use crate::catalog::ColumnCatalog;
use crate::types::{DatabaseId, SchemaId};

/// The logical plan of `create table`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
}

impl LogicalPlaner {
    pub fn plan_create_table(
        &self,
        stmt: BoundCreateTable,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::LogicalCreateTable(LogicalCreateTable {
            database_id: stmt.database_id,
            schema_id: stmt.schema_id,
            table_name: stmt.table_name,
            columns: stmt.columns,
        }))
    }
}
