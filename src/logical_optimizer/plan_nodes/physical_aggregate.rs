use std::fmt;

use super::PlanRef;
use crate::binder::{BoundAggCall, BoundExpr};
use crate::logical_optimizer::plan_nodes::logical_aggregate::LogicalAggregate;
use crate::physical_planner::*;

/// The physical plan of simple aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: PlanRef,
}

/// The physical plan of hash aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalHashAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: PlanRef,
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

impl fmt::Display for PhysicalHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashAgg: {} agg calls", self.agg_calls.len(),)?;
    }
}
impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashAgg: {} agg calls", self.agg_calls.len(),)?;
    }
}
