use super::*;
use crate::{
    binder::BoundCreateTable,
    catalog::{ColumnDesc, DatabaseId, SchemaId},
};
use itertools::Itertools;

/// The logical plan of `CREATE TABLE`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<(String, ColumnDesc)>,
}

impl LogicalPlaner {
    pub fn plan_create_table(
        &self,
        stmt: BoundCreateTable,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalCreateTable {
            database_id: stmt.database_id,
            schema_id: stmt.schema_id,
            table_name: stmt.table_name,
            columns: stmt.columns,
        }
        .into())
    }
}

impl Explain for LogicalCreateTable {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "CreateTable: name {}, columns [{}]",
            self.table_name,
            self.columns
                .iter()
                .map(|(name, col)| format!("{}: {:?}", name, col.datatype()))
                .join(", ")
        )
    }
}
