use super::*;
use crate::{binder::BoundExpr, logical_optimizer::plan_node::UnaryLogicalPlanNode};

/// The logical plan of project operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalProjection {
    fn get_child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn copy_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalProjection(LogicalProjection {
            child,
            project_expressions: self.project_expressions.clone(),
        })
        .into()
    }
}
