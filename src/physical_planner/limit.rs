use crate::logical_optimizer::plan_nodes::logical_limit::LogicalLimit;

use super::*;

/// The physical plan of limit operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_limit(&self, plan: LogicalLimit) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Limit(PhysicalLimit {
            offset: plan.offset,
            limit: plan.limit,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }
}

impl PlanExplainable for PhysicalLimit {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
