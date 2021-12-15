use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode};
use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::UnaryPlanNode;

/// The logical plan of project operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl UnaryPlanNode for LogicalProjection {
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
impl_plan_tree_node_for_unary! {LogicalProjection}

impl fmt::Display for LogicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalProjection: exprs {:?}", self.project_expressions)
    }
}
