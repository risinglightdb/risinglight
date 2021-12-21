use itertools::Itertools;

use super::*;
use crate::binder::BoundCreateTable;
use crate::catalog::{ColumnDesc, SchemaId};

/// The logical plan of `CREATE TABLE`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalCreateTable {
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<(String, ColumnDesc)>,
}

impl LogicalPlanner {
    pub fn plan_create_table(
        &self,
        stmt: BoundCreateTable,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalCreateTable {
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
            "CreateTable: name: {}, columns: [{}]",
            self.table_name,
            self.columns
                .iter()
                .map(|(name, col)| format!("{}: {:?}", name, col.datatype()))
                .join(", ")
        )
    }
}
