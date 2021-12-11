use super::*;
use crate::binder::{BoundDelete, BoundTableRef};
use crate::logical_optimizer::plan_nodes::logical_delete::LogicalDelete;
use crate::logical_optimizer::plan_nodes::logical_filter::LogicalFilter;

impl LogicalPlaner {
    pub fn plan_delete(&self, stmt: BoundDelete) -> Result<LogicalPlan, LogicalPlanError> {
        if let BoundTableRef::BaseTableRef { ref ref_id, .. } = stmt.from_table {
            if let Some(expr) = stmt.where_clause {
                let child = self.plan_table_ref(&stmt.from_table, true, false)?.into();
                Ok(LogicalPlan::LogicalDelete(LogicalDelete {
                    table_ref_id: *ref_id,
                    child: LogicalPlan::LogicalFilter(LogicalFilter { expr, child }).into(),
                }))
            } else {
                panic!("delete whole table is not supported yet")
            }
        } else {
            panic!("unsupported table")
        }
    }
}
