use std::fmt;

use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::logical_filter::LogicalFilter;
use crate::physical_planner::*;

/// The physical plan of filter operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalFilter {
    pub expr: BoundExpr,
    pub child: PlanRef,
}

impl PhysicalPlaner {
    pub fn plan_filter(&self, plan: LogicalFilter) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Filter(PhysicalFilter {
            expr: plan.expr,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }
}

impl fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)?;
    }
}
