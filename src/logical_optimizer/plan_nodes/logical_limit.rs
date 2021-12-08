use super::{LogicalPlan, LogicalPlanRef};
use crate::logical_optimizer::plan_nodes::UnaryLogicalPlanNode;

/// The logical plan of limit operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalLimit {
    fn child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalLimit(LogicalLimit {
            child,
            offset: self.offset,
            limit: self.limit,
        })
        .into()
    }
}
