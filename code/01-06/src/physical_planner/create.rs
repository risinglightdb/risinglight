use super::*;
use crate::catalog::{ColumnDesc, DatabaseId, SchemaId};
use crate::logical_planner::LogicalCreateTable;
use itertools::Itertools;

/// The physical plan of `CREATE TABLE`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<(String, ColumnDesc)>,
}

impl PhysicalPlaner {
    pub fn plan_create_table(
        &self,
        plan: &LogicalCreateTable,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalCreateTable {
            database_id: plan.database_id,
            schema_id: plan.schema_id,
            table_name: plan.table_name.clone(),
            columns: plan.columns.clone(),
        }
        .into())
    }
}

impl Explain for PhysicalCreateTable {
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
