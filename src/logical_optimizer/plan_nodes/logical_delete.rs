use crate::catalog::TableRefId;
use crate::logical_optimizer::plan_nodes::{LogicalPlan, UnaryLogicalPlanNode};

use super::LogicalPlanRef;

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
