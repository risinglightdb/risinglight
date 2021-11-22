use super::*;
use crate::binder::{BoundAggCall, BoundExpr};
use crate::logical_planner::LogicalHashAgg;

/// The physical plan of hash aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalHashAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_hash_agg(&self, plan: LogicalHashAgg) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::HashAgg(PhysicalHashAgg {
            agg_calls: plan.agg_calls,
            group_keys: plan.group_keys,
            child: Box::new(self.plan_inner(*plan.child)?),
        }))
    }
}

impl PlanExplainable for PhysicalHashAgg {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "HashAgg: {} agg calls", self.agg_calls.len(),)?;
        self.child.explain(level + 1, f)
    }
}
