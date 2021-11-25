use crate::logical_optimizer::plan_node::UnaryLogicalPlanNode;

use super::*;

/// The logical plan of limit operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalLimit {
    fn get_child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn copy_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalLimit(LogicalLimit {
            child,
            offset: self.offset,
            limit: self.limit,
        })
        .into()
    }
}
