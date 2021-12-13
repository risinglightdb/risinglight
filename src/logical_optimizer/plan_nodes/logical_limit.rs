use std::fmt;

use super::{Plan, PlanRef};
use crate::logical_optimizer::plan_nodes::UnaryLogicalPlanNode;

/// The logical plan of limit operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: PlanRef,
}

impl UnaryLogicalPlanNode for LogicalLimit {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalLimit(LogicalLimit {
            child,
            offset: self.offset,
            limit: self.limit,
        })
        .into()
    }
}
impl fmt::Display for LogicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
