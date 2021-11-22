use super::*;
use crate::binder::BoundAggCall;
use crate::logical_planner::LogicalSimpleAgg;

/// The physical plan of simple aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_simple_agg(
        &self,
        plan: LogicalSimpleAgg,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::SimpleAgg(PhysicalSimpleAgg {
            agg_calls: plan.agg_calls,
            child: Box::new(self.plan_inner(*plan.child)?),
        }))
    }
}

impl PlanExplainable for PhysicalSimpleAgg {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SimpleAgg: {:?}", self.agg_calls)?;
        self.child.explain(level + 1, f)
    }
}
