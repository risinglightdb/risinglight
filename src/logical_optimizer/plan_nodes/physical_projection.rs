use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryLogicalPlanNode};
use crate::binder::BoundExpr;

/// The physical plan of project operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: PlanRef,
}
impl UnaryLogicalPlanNode for PhysicalProjection {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalProjection(PhysicalProjection {
            child,
            project_expressions: self.project_expressions.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalProjection}
impl fmt::Display for PhysicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalProjection: exprs {:?}",
            self.project_expressions
        )
    }
}
