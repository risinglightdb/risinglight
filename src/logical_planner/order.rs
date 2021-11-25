use super::*;
use crate::{binder::BoundOrderBy, logical_optimizer::plan_node::UnaryLogicalPlanNode};

/// The logical plan of order.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalOrder {
    fn get_child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn copy_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalOrder(LogicalOrder {
            child,
            comparators: self.comparators.clone(),
        })
        .into()
    }
}
