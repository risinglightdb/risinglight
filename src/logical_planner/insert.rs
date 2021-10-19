use super::*;
use crate::binder::{BoundExpr, BoundInsert};
use crate::catalog::TableRefId;
use crate::types::ColumnId;

/// The logical plan of `insert values`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl LogicalPlaner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::Insert(LogicalInsert {
            table_ref_id: stmt.table_ref_id,
            column_ids: stmt.column_ids,
            values: stmt.values,
        }))
    }
}
