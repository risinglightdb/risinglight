use std::fmt;

use crate::logical_optimizer::plan_nodes::logical_explain::LogicalExplain;

/// The physical plan of `explain`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalExplain {
    pub plan: PlanRef,
}

impl PhysicalPlaner {
    pub fn plan_explain(&self, plan: LogicalExplain) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Explain(PhysicalExplain {
            plan: (self.plan_inner(plan.plan.as_ref().clone())?.into()),
        }))
    }
}

impl fmt::Display for PhysicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Huh, explain myself?")
    }
}
