use super::*;
use crate::binder::{BoundDelete, BoundTableRef};
use crate::catalog::TableRefId;

/// The logical plan of `delete`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalDelete {
    pub table_ref_id: TableRefId,
    pub filter: LogicalFilter,
}

impl LogicalPlaner {
    pub fn plan_delete(&self, stmt: BoundDelete) -> Result<LogicalPlan, LogicalPlanError> {
        if let BoundTableRef::BaseTableRef { ref ref_id, .. } = stmt.from_table {
            if let Some(expr) = stmt.where_clause {
                let child = self.plan_table_ref(&stmt.from_table, true, false)?.into();
                Ok(LogicalPlan::Delete(LogicalDelete {
                    table_ref_id: *ref_id,
                    filter: LogicalFilter { expr, child },
                }))
            } else {
                panic!("delete whole table is not supported yet")
            }
        } else {
            panic!("unsupported table")
        }
    }
}
