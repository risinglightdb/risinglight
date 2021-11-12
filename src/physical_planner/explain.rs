use crate::logical_planner::LogicalExplain;

use super::*;

/// The physical plan of `explain`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalExplain {
    pub plan: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_explain(&self, plan: LogicalExplain) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Explain(PhysicalExplain {
            plan: Box::new(self.plan_inner(*plan.plan)?),
        }))
    }
}

impl PlanExplainable for PhysicalExplain {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Huh, explain myself?")
    }
}
