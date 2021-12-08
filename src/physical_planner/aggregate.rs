use super::*;
use crate::{
    binder::{BoundAggCall, BoundExpr},
    logical_optimizer::plan_nodes::logical_aggregate::LogicalAggregate,
};

/// The physical plan of simple aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: Box<PhysicalPlan>,
}

/// The physical plan of hash aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalHashAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_aggregate(
        &self,
        plan: LogicalAggregate,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        if plan.group_keys.is_empty() {
            Ok(PhysicalPlan::SimpleAgg(PhysicalSimpleAgg {
                agg_calls: plan.agg_calls,
                child: self.plan_inner(plan.child.as_ref().clone())?.into(),
            }))
        } else {
            Ok(PhysicalPlan::HashAgg(PhysicalHashAgg {
                agg_calls: plan.agg_calls,
                group_keys: plan.group_keys,
                child: self.plan_inner(plan.child.as_ref().clone())?.into(),
            }))
        }
    }
}

impl PlanExplainable for PhysicalSimpleAgg {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SimpleAgg: {:?}", self.agg_calls)?;
        self.child.explain(level + 1, f)
    }
}

impl PlanExplainable for PhysicalHashAgg {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "HashAgg: {} agg calls", self.agg_calls.len(),)?;
        self.child.explain(level + 1, f)
    }
}
