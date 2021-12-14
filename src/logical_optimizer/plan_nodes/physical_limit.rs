use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryLogicalPlanNode};

/// The physical plan of limit operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: PlanRef,
}
impl UnaryLogicalPlanNode for PhysicalLimit {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalLimit(PhysicalLimit {
            child,
            offset: self.offset,
            limit: self.limit,
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalLimit}

impl fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
