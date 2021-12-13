use std::fmt;

use crate::logical_optimizer::plan_nodes::{Plan, PlanRef, UnaryLogicalPlanNode};

/// The logical plan of `explain`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalExplain {
    pub plan: PlanRef,
}

impl UnaryLogicalPlanNode for LogicalExplain {
    fn child(&self) -> PlanRef {
        self.plan.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalExplain(LogicalExplain { plan: child }).into()
    }
}

impl fmt::Display for LogicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Huh, explain myself?")
    }
}
