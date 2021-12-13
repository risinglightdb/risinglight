use std::fmt;

use itertools::Itertools;

use crate::catalog::ColumnCatalog;
use crate::logical_optimizer::plan_nodes::logical_create_table::LogicalCreateTable;
use crate::physical_planner::*;
use crate::types::{DatabaseId, SchemaId};

/// The physical plan of `create table`.
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

impl fmt::Display for PhysicalCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCreateTable: table {}, columns [{}]",
            self.table_name,
            self.columns
                .iter()
                .map(|x| format!("{}:{:?}", x.name(), x.datatype()))
                .join(", ")
        )
    }
}
