use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryLogicalPlanNode};
use crate::binder::BoundOrderBy;

/// The physical plan of order.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: PlanRef,
}

impl UnaryLogicalPlanNode for PhysicalOrder {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalOrder(PhysicalOrder {
            child,
            comparators: self.comparators.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalOrder}

impl fmt::Display for PhysicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalOrder: {:?}", self.comparators)
    }
}
