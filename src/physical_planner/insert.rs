use super::*;
use crate::binder::BoundExpr;
use crate::catalog::TableRefId;
use crate::logical_planner::LogicalInsert;
use crate::types::ColumnId;

/// The physical plan of `insert values`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    /// The rows to be inserted.
    ///
    /// Each row is composed of multiple values,
    /// each value is represented by an expression.
    pub values: Vec<Vec<BoundExpr>>,
}

impl PhysicalPlaner {
    pub fn plan_insert(&self, stmt: LogicalInsert) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Insert(PhysicalInsert {
            table_ref_id: stmt.table_ref_id,
            column_ids: stmt.column_ids,
            values: stmt.values,
        }))
    }
}
