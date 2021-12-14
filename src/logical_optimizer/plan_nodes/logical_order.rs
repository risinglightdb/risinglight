use std::fmt;

use super::{Plan, PlanRef};
use crate::binder::BoundOrderBy;
use crate::logical_optimizer::plan_nodes::UnaryLogicalPlanNode;

/// The logical plan of order.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: PlanRef,
}

impl UnaryLogicalPlanNode for LogicalOrder {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalOrder(LogicalOrder {
            child,
            comparators: self.comparators.clone(),
        })
        .into()
    }
}

impl fmt::Display for LogicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalOrder: {:?}", self.comparators)
    }
}
