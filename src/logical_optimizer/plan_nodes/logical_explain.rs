use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryPlanNode};

/// The logical plan of `explain`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalExplain {
    pub plan: PlanRef,
}

impl UnaryPlanNode for LogicalExplain {
    fn child(&self) -> PlanRef {
        self.plan.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalExplain(LogicalExplain { plan: child }).into()
    }
}
impl_plan_tree_node_for_unary! {LogicalExplain}

impl fmt::Display for LogicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Huh, explain myself?")
    }
}
