use super::{LogicalPlan, LogicalPlanRef};
use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::UnaryLogicalPlanNode;

/// The logical plan of project operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalProjection {
    fn child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalProjection(LogicalProjection {
            child,
            project_expressions: self.project_expressions.clone(),
        })
        .into()
    }
}
