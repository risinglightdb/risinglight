use super::*;
use crate::catalog::ColumnCatalog;
use crate::logical_planner::LogicalCreateTable;
use crate::types::{DatabaseId, SchemaId};

#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
}

impl PhysicalPlaner {
    pub fn plan_create_table(
        &self,
        plan: LogicalCreateTable,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::CreateTable(PhysicalCreateTable {
            database_id: plan.database_id,
            schema_id: plan.schema_id,
            table_name: plan.table_name,
            columns: plan.columns,
        }))
    }
}
