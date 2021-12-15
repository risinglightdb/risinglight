use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryPlanNode};
use crate::binder::BoundExpr;

/// The physical plan of filter operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalFilter {
    pub expr: BoundExpr,
    pub child: PlanRef,
}

impl UnaryPlanNode for PhysicalFilter {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalFilter(PhysicalFilter {
            child,
            expr: self.expr.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalFilter}

impl fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)
    }
}
