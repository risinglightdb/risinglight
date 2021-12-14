use std::fmt;

use crate::logical_optimizer::plan_nodes::logical_limit::LogicalLimit;

/// The physical plan of limit operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: PlanRef,
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

impl fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
