use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode};
use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::UnaryPlanNode;

/// The logical plan of filter operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalFilter {
    pub expr: BoundExpr,
    pub child: PlanRef,
}

impl UnaryPlanNode for LogicalFilter {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalFilter(LogicalFilter {
            child,
            expr: self.expr.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {LogicalFilter}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)
    }
}
