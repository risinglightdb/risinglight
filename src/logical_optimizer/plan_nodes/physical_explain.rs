use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryLogicalPlanNode};

/// The physical plan of `explain`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalExplain {
    pub plan: PlanRef,
}
impl UnaryLogicalPlanNode for PhysicalExplain {
    fn child(&self) -> PlanRef {
        self.plan.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalExplain(PhysicalExplain { plan: child }).into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalExplain}

impl fmt::Display for PhysicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Huh, explain myself?")
    }
}
