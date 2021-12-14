use std::fmt;

use super::{Plan, PlanRef};
use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::UnaryLogicalPlanNode;

/// The logical plan of project operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl UnaryLogicalPlanNode for LogicalProjection {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalProjection(LogicalProjection {
            child,
            project_expressions: self.project_expressions.clone(),
        })
        .into()
    }
}
impl fmt::Display for LogicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalProjection: exprs {:?}", self.project_expressions)
    }
}
