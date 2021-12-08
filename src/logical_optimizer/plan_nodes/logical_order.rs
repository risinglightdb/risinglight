use super::{LogicalPlan, LogicalPlanRef};
use crate::{binder::BoundOrderBy, logical_optimizer::plan_nodes::UnaryLogicalPlanNode};

/// The logical plan of order.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalOrder {
    fn child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalOrder(LogicalOrder {
            child,
            comparators: self.comparators.clone(),
        })
        .into()
    }
}
