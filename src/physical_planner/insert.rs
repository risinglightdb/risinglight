use super::*;
use crate::binder::BoundExpr;
use crate::catalog::TableRefId;
use crate::logical_planner::LogicalInsert;
use crate::types::ColumnId;

/// The physical plan of `insert`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: Box<PhysicalPlan>,
}

/// The physical plan of `values`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalValues {
    pub values: Vec<Vec<BoundExpr>>,
}

impl PhysicalPlaner {
    pub fn plan_insert(&self, stmt: LogicalInsert) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Insert(PhysicalInsert {
            table_ref_id: stmt.table_ref_id,
            column_ids: stmt.column_ids,
            child: Box::new(PhysicalPlan::Values(PhysicalValues {
                values: stmt.values,
            })),
        }))
    }
}
