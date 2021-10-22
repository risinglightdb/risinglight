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
            plan: Box::new(self.plan(*plan.plan)?),
        }))
    }
}
