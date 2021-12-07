use super::{LogicalPlan, LogicalPlanRef};
use crate::{binder::BoundExpr, logical_optimizer::plan_nodes::UnaryLogicalPlanNode};

/// The logical plan of filter operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalFilter {
    pub expr: BoundExpr,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalFilter {
    fn child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalFilter(LogicalFilter {
            child,
            expr: self.expr.clone(),
        })
        .into()
    }
}
