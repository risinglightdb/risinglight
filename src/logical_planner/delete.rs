use super::*;
use crate::binder::{BoundDelete, BoundTableRef};
use crate::catalog::TableRefId;
use crate::logical_optimizer::plan_node::UnaryLogicalPlanNode;

/// The logical plan of `delete`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalDelete {
    pub table_ref_id: TableRefId,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalDelete {
    fn get_child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn copy_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalDelete(LogicalDelete {
            table_ref_id: self.table_ref_id,
            child,
        })
        .into()
    }
}

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
